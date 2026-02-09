# Expired Domain Link Finder (Apify Actor)

This project is an [Apify Actor](https://docs.apify.com/platform/actors) that:

1. Crawls one or more target domains (optionally including subdomains).
2. Extracts outgoing links from crawled pages.
3. Checks which outgoing links are broken (HTTP 4xx/5xx or request failure).
4. Optionally applies domain-level heuristics to flag links that point to likely expired domains.

## Input

- `startUrls`: One or more root URLs/domains to crawl.
- `startUrl`: Optional legacy single URL input (backward-compatible).
- `maxPages`: Maximum pages to crawl per start URL. Use `0` to crawl all discovered in-domain pages.
- `maxCrawlDepth`: Max click depth. `0` = unlimited.
- `followSubdomains`: Include subdomains in crawl scope.
- `maxConcurrency`: Crawl concurrency.
- `requestTimeoutSecs`: Crawl request timeout.
- `includeNofollow`: Include nofollow links in outgoing-link checks.
- `checkDomainExpiry`: Run DNS/root checks to estimate if a domain is likely expired.
- `checkTimeoutSecs`: Timeout for outgoing-link/domain checks.
- `linkCheckConcurrency`: Concurrency for outgoing-link checks.
- `useApifyProxy`: Toggle Apify proxy usage on/off.
- `useResidentialProxies`: If true, proxy group `RESIDENTIAL` is requested.
- `apifyProxyGroups`: Optional extra Apify proxy groups.
- `apifyProxyCountry`: Optional proxy country code (for example `US`).

## Output

### Dataset items

The dataset contains one item per broken outgoing link:

- `targetUrl`, `targetDomain`
- `sourceUrls`, `sourceUrlCount`
- `linkStatusCode`, `linkErrorCode`, `linkErrorMessage`
- `isLikelyExpiredDomain`, `expiryReason`, `expiryConfidence`
- `domainDiagnostics`

### Key-value store

- `SUMMARY`: Crawl and check statistics, plus failed crawl requests.
- `EXPIRED_DOMAINS`: Unique likely expired domains found in the scan (deduplicated).
- `DOMAIN_CANDIDATES`: Unique deduplicated domains from broken outgoing links, each with `isLikelyExpiredDomain`.

## Residential proxies on Apify

This actor supports a native on/off toggle:

- Set `useApifyProxy` to `true` to enable Apify Proxy.
- Set `useResidentialProxies` to `true` to request the `RESIDENTIAL` group.

If your Apify account has residential proxy access, billing/usage is handled by Apify natively.

## Local development

```bash
npm install
npm start
```

Run on Apify by importing this repository as an Actor source.

## Sample `EXPIRED_DOMAINS` item

```json
{
  "domain": "example-expired-domain.com",
  "isLikelyExpiredDomain": true,
  "brokenUrlCount": 4,
  "brokenUrls": [
    "http://example-expired-domain.com/",
    "https://example-expired-domain.com/about"
  ],
  "foundOnDomainCount": 2,
  "foundOnDomains": [
    "site-a.com",
    "site-b.org"
  ],
  "foundOnStartUrlCount": 2,
  "foundOnStartUrls": [
    "https://site-a.com/",
    "https://site-b.org/"
  ],
  "sourceUrlCount": 5,
  "sourceUrls": [
    "https://site-a.com/resources",
    "https://site-b.org/links"
  ],
  "expiryReason": "dns_unresolved",
  "expiryConfidence": "high",
  "domainDiagnostics": {
    "domain": "example-expired-domain.com",
    "dnsResolvable": false
  }
}
```
