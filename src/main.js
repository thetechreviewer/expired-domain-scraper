import { Actor, log } from 'apify';
import { CheerioCrawler, gotScraping } from 'crawlee';
import { promises as dns } from 'node:dns';
import { parse as parseTld } from 'tldts';

function normalizeHttpUrl(rawUrl, baseUrl) {
  if (!rawUrl) return null;

  const trimmed = String(rawUrl).trim();
  if (!trimmed || trimmed.startsWith('#')) return null;

  try {
    const url = new URL(trimmed, baseUrl);
    if (url.protocol !== 'http:' && url.protocol !== 'https:') return null;
    url.hash = '';
    return url.toString();
  } catch {
    return null;
  }
}

function normalizeStartUrl(rawUrl) {
  if (!rawUrl) return null;
  const trimmed = String(rawUrl).trim();
  if (!trimmed) return null;

  const withProtocol = /^https?:\/\//i.test(trimmed) ? trimmed : `https://${trimmed}`;
  return normalizeHttpUrl(withProtocol, withProtocol);
}

function getRegistrableDomain(hostname) {
  const parsed = parseTld(hostname);
  return parsed.domain || hostname;
}

function isInternalUrl(candidateUrl, baseHostname, baseDomain, followSubdomains) {
  const candidate = new URL(candidateUrl);
  if (followSubdomains) {
    return getRegistrableDomain(candidate.hostname) === baseDomain;
  }

  return candidate.hostname === baseHostname;
}

function withDefault(input, key, fallback) {
  return input[key] === undefined || input[key] === null ? fallback : input[key];
}

function toSafeInt(value, fallback, min = 1) {
  const parsed = Number.parseInt(String(value), 10);
  if (Number.isNaN(parsed)) return fallback;
  return parsed < min ? min : parsed;
}

function randomDelay(minMs, maxMs) {
  const ms = Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchHttpStatus(url, method, timeoutSecs, proxyConfiguration) {
  try {
    const proxyUrl = proxyConfiguration ? await proxyConfiguration.newUrl(url) : undefined;
    const response = await gotScraping({
      url,
      method,
      proxyUrl,
      throwHttpErrors: false,
      followRedirect: true,
      maxRedirects: 10,
      timeout: { request: timeoutSecs * 1000 },
      retry: { limit: 1 },
      headerGeneratorOptions: {
        browsers: [{ name: 'chrome', minVersion: 116 }],
      },
    });

    return {
      ok: true,
      statusCode: response.statusCode,
      finalUrl: response.url,
    };
  } catch (error) {
    return {
      ok: false,
      errorCode: error?.code || 'REQUEST_FAILED',
      errorMessage: error?.message || 'Unknown request error',
    };
  }
}

async function checkOutgoingUrl(url, timeoutSecs, proxyConfiguration) {
  const headResult = await fetchHttpStatus(url, 'HEAD', timeoutSecs, proxyConfiguration);

  if (headResult.ok && ![405, 501].includes(headResult.statusCode)) {
    return {
      url,
      checkMethod: 'HEAD',
      statusCode: headResult.statusCode,
      broken: headResult.statusCode >= 400,
      finalUrl: headResult.finalUrl,
      errorCode: null,
      errorMessage: null,
    };
  }

  const getResult = await fetchHttpStatus(url, 'GET', timeoutSecs, proxyConfiguration);
  if (getResult.ok) {
    return {
      url,
      checkMethod: 'GET',
      statusCode: getResult.statusCode,
      broken: getResult.statusCode >= 400,
      finalUrl: getResult.finalUrl,
      errorCode: null,
      errorMessage: null,
    };
  }

  if (headResult.ok) {
    return {
      url,
      checkMethod: 'HEAD',
      statusCode: headResult.statusCode,
      broken: headResult.statusCode >= 400,
      finalUrl: headResult.finalUrl,
      errorCode: null,
      errorMessage: null,
    };
  }

  return {
    url,
    checkMethod: 'GET',
    statusCode: null,
    broken: true,
    finalUrl: null,
    errorCode: getResult.errorCode || headResult.errorCode,
    errorMessage: getResult.errorMessage || headResult.errorMessage,
  };
}

async function checkDomainExpiry(domain, timeoutSecs, proxyConfiguration, cache) {
  if (cache.has(domain)) return cache.get(domain);

  const result = {
    domain,
    dnsResolvable: null,
    dnsErrorCode: null,
    rootHttpsStatusCode: null,
    rootHttpStatusCode: null,
    rootHttpsErrorCode: null,
    rootHttpErrorCode: null,
    isLikelyExpired: false,
    expiryReason: 'insufficient_signal',
    confidence: 'low',
  };

  try {
    const resolved = await dns.lookup(domain, { all: true });
    result.dnsResolvable = Array.isArray(resolved) && resolved.length > 0;
  } catch (error) {
    result.dnsResolvable = false;
    result.dnsErrorCode = error?.code || 'DNS_LOOKUP_FAILED';

    if (['ENOTFOUND', 'ENODATA', 'ESERVFAIL', 'SERVFAIL', 'NOTFOUND'].includes(result.dnsErrorCode)) {
      result.isLikelyExpired = true;
      result.expiryReason = 'dns_unresolved';
      result.confidence = 'high';
      cache.set(domain, result);
      return result;
    }
  }

  const httpsRoot = `https://${domain}/`;
  const httpRoot = `http://${domain}/`;

  const httpsResult = await checkOutgoingUrl(httpsRoot, timeoutSecs, proxyConfiguration);
  const httpResult = await checkOutgoingUrl(httpRoot, timeoutSecs, proxyConfiguration);

  result.rootHttpsStatusCode = httpsResult.statusCode;
  result.rootHttpStatusCode = httpResult.statusCode;
  result.rootHttpsErrorCode = httpsResult.errorCode;
  result.rootHttpErrorCode = httpResult.errorCode;

  const httpsBroken = httpsResult.broken;
  const httpBroken = httpResult.broken;

  if (httpsBroken && httpBroken) {
    const statusCodes = [httpsResult.statusCode, httpResult.statusCode].filter((s) => typeof s === 'number');
    const allNotFoundLike = statusCodes.length > 0 && statusCodes.every((s) => [404, 410].includes(s));

    if (allNotFoundLike) {
      result.isLikelyExpired = true;
      result.expiryReason = 'root_not_found';
      result.confidence = 'medium';
    } else if (!result.dnsResolvable) {
      result.isLikelyExpired = true;
      result.expiryReason = 'dns_and_http_failures';
      result.confidence = 'medium';
    }
  }

  cache.set(domain, result);
  return result;
}

async function mapWithConcurrency(items, concurrency, worker) {
  const results = new Array(items.length);
  let cursor = 0;
  const workerCount = Math.min(concurrency, items.length);

  await Promise.all(
    Array.from({ length: workerCount }, async () => {
      while (cursor < items.length) {
        const index = cursor;
        cursor += 1;
        results[index] = await worker(items[index], index);
      }
    }),
  );

  return results;
}

await Actor.init();

try {
  const input = (await Actor.getInput()) || {};

  const candidateStartUrls = [];
  if (Array.isArray(input.startUrls)) candidateStartUrls.push(...input.startUrls);
  if (input.startUrl) candidateStartUrls.push(input.startUrl);

  const startUrls = Array.from(
    new Set(
      candidateStartUrls
        .map(normalizeStartUrl)
        .filter(Boolean),
    ),
  );

  if (startUrls.length === 0) {
    throw new Error('Input requires at least one valid URL in "startUrls" or "startUrl".');
  }

  const maxPages = toSafeInt(withDefault(input, 'maxPages', 200), 200, 0);
  const maxCrawlDepth = toSafeInt(withDefault(input, 'maxCrawlDepth', 0), 0, 0);
  const followSubdomains = Boolean(withDefault(input, 'followSubdomains', true));
  const maxConcurrency = toSafeInt(withDefault(input, 'maxConcurrency', 20), 20, 1);
  const requestTimeoutSecs = toSafeInt(withDefault(input, 'requestTimeoutSecs', 30), 30, 5);
  const includeNofollow = Boolean(withDefault(input, 'includeNofollow', true));
  const checkDomainExpiryEnabled = Boolean(withDefault(input, 'checkDomainExpiry', true));
  const checkTimeoutSecs = toSafeInt(withDefault(input, 'checkTimeoutSecs', 20), 20, 3);
  const linkCheckConcurrency = toSafeInt(withDefault(input, 'linkCheckConcurrency', 30), 30, 1);
  const useApifyProxy = Boolean(withDefault(input, 'useApifyProxy', false));
  const useResidentialProxies = Boolean(withDefault(input, 'useResidentialProxies', false));
  const apifyProxyGroups = Array.isArray(input.apifyProxyGroups) ? input.apifyProxyGroups.filter(Boolean) : [];
  const apifyProxyCountry = input.apifyProxyCountry ? String(input.apifyProxyCountry).toUpperCase() : undefined;
  const maxRequestsPerMinute = toSafeInt(withDefault(input, 'maxRequestsPerMinute', 60), 60, 0);
  const requestDelayMin = toSafeInt(withDefault(input, 'requestDelayMin', 500), 500, 0);
  const requestDelayMax = toSafeInt(withDefault(input, 'requestDelayMax', 3000), 3000, 0);
  const respectRobotsTxt = Boolean(withDefault(input, 'respectRobotsTxt', true));
  const excludeDomains = new Set(
    (Array.isArray(input.excludeDomains) ? input.excludeDomains : [])
      .map((d) => String(d).trim().toLowerCase())
      .filter(Boolean)
      .map((d) => getRegistrableDomain(d)),
  );
  const minBrokenLinksPerDomain = toSafeInt(withDefault(input, 'minBrokenLinksPerDomain', 1), 1, 1);
  const extractAnchorText = Boolean(withDefault(input, 'extractAnchorText', true));

  const proxyGroups = [...apifyProxyGroups];
  if (useResidentialProxies && !proxyGroups.includes('RESIDENTIAL')) {
    proxyGroups.push('RESIDENTIAL');
  }

  const proxyConfiguration = useApifyProxy
    ? await Actor.createProxyConfiguration({
        groups: proxyGroups.length > 0 ? proxyGroups : undefined,
        countryCode: apifyProxyCountry,
      })
    : undefined;

  const externalLinks = new Map();
  const crawlErrors = [];
  const crawlByDomain = [];
  let totalCrawledPages = 0;
  const brokenOutgoingLinks = [];
  const domainCandidatesMap = new Map();
  let linksChecked = 0;
  let phase = 'crawling';

  function buildDomainCandidates() {
    return Array.from(domainCandidatesMap.values())
      .filter((entry) => entry.brokenUrlCount >= minBrokenLinksPerDomain)
      .map((entry) => ({
        domain: entry.domain,
        isLikelyExpiredDomain: entry.isLikelyExpiredDomain,
        brokenUrlCount: entry.brokenUrlCount,
        brokenUrls: Array.from(entry.brokenUrls),
        anchorTexts: Array.from(entry.anchorTexts),
        foundOnDomainCount: entry.foundOnDomains.size,
        foundOnDomains: Array.from(entry.foundOnDomains),
        foundOnStartUrlCount: entry.foundOnStartUrls.size,
        foundOnStartUrls: Array.from(entry.foundOnStartUrls),
        sourceUrlCount: entry.sourceUrls.size,
        sourceUrls: Array.from(entry.sourceUrls),
        expiryReason: entry.expiryReason,
        expiryConfidence: entry.expiryConfidence,
        domainDiagnostics: entry.domainDiagnostics,
      }))
      .sort((a, b) => {
        if (a.isLikelyExpiredDomain !== b.isLikelyExpiredDomain) {
          return Number(b.isLikelyExpiredDomain) - Number(a.isLikelyExpiredDomain);
        }
        return b.brokenUrlCount - a.brokenUrlCount;
      });
  }

  async function savePartialResults() {
    const domainCandidates = buildDomainCandidates();
    const expiredDomains = domainCandidates.filter((item) => item.isLikelyExpiredDomain);
    const externalEntryCount = externalLinks.size;

    await Actor.setValue('EXPIRED_DOMAINS', expiredDomains);
    await Actor.setValue('DOMAIN_CANDIDATES', domainCandidates);
    await Actor.setValue('SUMMARY', {
      status: phase === 'done' ? 'complete' : 'partial',
      phase,
      input: {
        startUrls,
        maxPages,
        maxCrawlDepth,
        followSubdomains,
        maxConcurrency,
        requestTimeoutSecs,
        maxRequestsPerMinute,
        requestDelayMin,
        requestDelayMax,
        respectRobotsTxt,
        includeNofollow,
        checkDomainExpiry: checkDomainExpiryEnabled,
        checkTimeoutSecs,
        linkCheckConcurrency,
        excludeDomains: Array.from(excludeDomains),
        minBrokenLinksPerDomain,
        extractAnchorText,
        useApifyProxy,
        useResidentialProxies,
        apifyProxyGroups: proxyGroups,
        apifyProxyCountry,
      },
      stats: {
        scannedDomains: startUrls.length,
        crawledPages: totalCrawledPages,
        uniqueExternalLinksFound: externalEntryCount,
        linksChecked,
        brokenOutgoingLinksFound: brokenOutgoingLinks.length,
        uniqueDomainCandidates: domainCandidates.length,
        likelyExpiredDomainsFound: expiredDomains.length,
        crawlFailures: crawlErrors.length,
      },
      crawlByDomain,
      crawlFailures: crawlErrors,
    });

    log.info('Saved partial results', {
      phase,
      crawledPages: totalCrawledPages,
      linksChecked,
      brokenLinks: brokenOutgoingLinks.length,
      expiredDomains: expiredDomains.length,
    });
  }

  // Save results every 30 seconds so partial data survives timeouts
  const saveInterval = setInterval(() => savePartialResults().catch(() => {}), 30_000);

  // Save results if Apify is about to kill the actor
  Actor.on('aborting', async () => {
    log.info('Actor aborting — saving partial results before exit...');
    clearInterval(saveInterval);
    await savePartialResults();
  });

  log.info('Starting crawl across input domains', {
    startUrlCount: startUrls.length,
    maxPagesPerDomain: maxPages > 0 ? maxPages : 'all discovered pages',
    maxCrawlDepth: maxCrawlDepth > 0 ? maxCrawlDepth : 'unlimited',
    followSubdomains,
    maxRequestsPerMinute: maxRequestsPerMinute > 0 ? maxRequestsPerMinute : 'unlimited',
    requestDelay: `${requestDelayMin}-${requestDelayMax}ms`,
    respectRobotsTxt,
    excludedDomains: excludeDomains.size,
    useApifyProxy,
    proxyGroups,
    apifyProxyCountry,
  });

  for (let i = 0; i < startUrls.length; i += 1) {
    const startUrl = startUrls[i];
    const parsedStartUrl = new URL(startUrl);
    const baseHostname = parsedStartUrl.hostname;
    const baseDomain = getRegistrableDomain(baseHostname);

    const requestQueue = await Actor.openRequestQueue(`scan-${i + 1}-${Date.now()}`);
    await requestQueue.addRequest({ url: startUrl, userData: { depth: 0 } });

    let crawledPagesForDomain = 0;

    const crawler = new CheerioCrawler({
      requestQueue,
      proxyConfiguration,
      maxConcurrency,
      maxRequestsPerCrawl: maxPages > 0 ? maxPages : undefined,
      maxRequestsPerMinute: maxRequestsPerMinute > 0 ? maxRequestsPerMinute : undefined,
      requestHandlerTimeoutSecs: requestTimeoutSecs,
      useSessionPool: true,
      respectRobotsTxtFile: respectRobotsTxt,
      async requestHandler({ request, $, contentType }) {
        if (requestDelayMax > 0) {
          await randomDelay(requestDelayMin, requestDelayMax);
        }
        const pageUrl = request.loadedUrl || request.url;
        const depth = toSafeInt(request.userData?.depth ?? 0, 0, 0);

        if (!contentType?.type?.includes('text/html')) return;
        if (!isInternalUrl(pageUrl, baseHostname, baseDomain, followSubdomains)) return;

        crawledPagesForDomain += 1;
        totalCrawledPages += 1;

        const internalToEnqueue = new Map();

        $('a[href]').each((_, el) => {
          const href = $(el).attr('href');
          const normalized = normalizeHttpUrl(href, pageUrl);
          if (!normalized) return;

          if (!includeNofollow) {
            const relValue = ($(el).attr('rel') || '').toLowerCase();
            if (relValue.split(/\s+/).includes('nofollow')) return;
          }

          if (isInternalUrl(normalized, baseHostname, baseDomain, followSubdomains)) {
            const nextDepth = depth + 1;
            if (maxCrawlDepth === 0 || nextDepth <= maxCrawlDepth) {
              internalToEnqueue.set(normalized, nextDepth);
            }
            return;
          }

          const parsed = new URL(normalized);
          const targetDomain = getRegistrableDomain(parsed.hostname);

          if (excludeDomains.has(targetDomain)) return;

          const anchorText = extractAnchorText
            ? $(el).text().trim().substring(0, 500) || null
            : null;

          const existing = externalLinks.get(normalized);
          if (existing) {
            existing.sourcePages.add(pageUrl);
            existing.foundOnDomains.add(baseDomain);
            existing.foundOnStartUrls.add(startUrl);
            if (anchorText) existing.anchorTexts.add(anchorText);
          } else {
            externalLinks.set(normalized, {
              targetUrl: normalized,
              targetDomain,
              sourcePages: new Set([pageUrl]),
              foundOnDomains: new Set([baseDomain]),
              foundOnStartUrls: new Set([startUrl]),
              anchorTexts: new Set(anchorText ? [anchorText] : []),
            });
          }
        });

        if (internalToEnqueue.size > 0) {
          await requestQueue.addRequests(
            Array.from(internalToEnqueue.entries()).map(([url, nextDepth]) => ({
              url,
              userData: { depth: nextDepth },
            })),
          );
        }
      },
      failedRequestHandler({ request, error }) {
        crawlErrors.push({
          scanStartUrl: startUrl,
          url: request.url,
          errorMessage: error?.message || 'Request failed',
        });
      },
    });

    log.info('Crawling domain', {
      startUrl,
      position: `${i + 1}/${startUrls.length}`,
      maxPages: maxPages > 0 ? maxPages : 'all discovered pages',
      maxCrawlDepth: maxCrawlDepth > 0 ? maxCrawlDepth : 'unlimited',
    });

    await crawler.run();

    crawlByDomain.push({
      startUrl,
      scanDomain: baseDomain,
      crawledPages: crawledPagesForDomain,
    });
  }

  const externalEntries = Array.from(externalLinks.values());
  phase = 'checking_links';
  log.info('Finished crawling. Checking external links.', {
    totalCrawledPages,
    externalLinkCount: externalEntries.length,
  });

  // Save crawl progress before starting link checks
  await savePartialResults();

  const domainCheckCache = new Map();

  function addToDomainCandidates(item) {
    const existing = domainCandidatesMap.get(item.targetDomain);
    if (existing) {
      existing.brokenUrlCount += 1;
      existing.brokenUrls.add(item.targetUrl);
      existing.foundOnDomains = new Set([...existing.foundOnDomains, ...item.foundOnDomains]);
      existing.foundOnStartUrls = new Set([...existing.foundOnStartUrls, ...item.foundOnStartUrls]);
      existing.sourceUrls = new Set([...existing.sourceUrls, ...item.sourceUrls]);
      for (const a of item.anchorTexts) existing.anchorTexts.add(a);
      existing.isLikelyExpiredDomain = existing.isLikelyExpiredDomain || Boolean(item.isLikelyExpiredDomain);

      if (item.isLikelyExpiredDomain && !existing.expiryReason) {
        existing.expiryReason = item.expiryReason;
        existing.expiryConfidence = item.expiryConfidence;
        existing.domainDiagnostics = item.domainDiagnostics;
      }
    } else {
      domainCandidatesMap.set(item.targetDomain, {
        domain: item.targetDomain,
        isLikelyExpiredDomain: Boolean(item.isLikelyExpiredDomain),
        brokenUrlCount: 1,
        brokenUrls: new Set([item.targetUrl]),
        foundOnDomains: new Set(item.foundOnDomains),
        foundOnStartUrls: new Set(item.foundOnStartUrls),
        sourceUrls: new Set(item.sourceUrls),
        anchorTexts: new Set(item.anchorTexts),
        expiryReason: item.isLikelyExpiredDomain ? item.expiryReason : null,
        expiryConfidence: item.isLikelyExpiredDomain ? item.expiryConfidence : null,
        domainDiagnostics: item.isLikelyExpiredDomain ? item.domainDiagnostics : null,
      });
    }
  }

  await mapWithConcurrency(externalEntries, linkCheckConcurrency, async (link) => {
    const linkStatus = await checkOutgoingUrl(link.targetUrl, checkTimeoutSecs, proxyConfiguration);
    linksChecked += 1;

    if (!linkStatus.broken) return;

    const domainStatus = checkDomainExpiryEnabled
      ? await checkDomainExpiry(link.targetDomain, checkTimeoutSecs, proxyConfiguration, domainCheckCache)
      : null;

    const result = {
      targetUrl: link.targetUrl,
      targetDomain: link.targetDomain,
      anchorTexts: Array.from(link.anchorTexts),
      sourceUrlCount: link.sourcePages.size,
      sourceUrls: Array.from(link.sourcePages),
      foundOnDomains: Array.from(link.foundOnDomains),
      foundOnStartUrls: Array.from(link.foundOnStartUrls),
      linkCheckMethod: linkStatus.checkMethod,
      linkStatusCode: linkStatus.statusCode,
      linkFinalUrl: linkStatus.finalUrl,
      linkErrorCode: linkStatus.errorCode,
      linkErrorMessage: linkStatus.errorMessage,
      isBrokenOutgoingLink: true,
      isLikelyExpiredDomain: domainStatus ? domainStatus.isLikelyExpired : null,
      expiryReason: domainStatus ? domainStatus.expiryReason : null,
      expiryConfidence: domainStatus ? domainStatus.confidence : null,
      domainDiagnostics: domainStatus,
    };

    brokenOutgoingLinks.push(result);
    addToDomainCandidates(result);

    // Push each broken link to the dataset immediately
    await Actor.pushData([result]);
  });

  // Final save
  clearInterval(saveInterval);
  phase = 'done';
  await savePartialResults();

  log.info('Actor finished', {
    crawledPages: totalCrawledPages,
    linksChecked,
    brokenLinks: brokenOutgoingLinks.length,
    expiredDomains: buildDomainCandidates().filter((d) => d.isLikelyExpiredDomain).length,
  });
} catch (error) {
  clearInterval(saveInterval);
  log.error('Actor failed', { message: error?.message, stack: error?.stack });
  await savePartialResults().catch(() => {});
  throw error;
} finally {
  await Actor.exit();
}
