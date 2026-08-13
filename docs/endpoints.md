# Endpoints

An endpoint turns an upstream API into a cached document. Call it, and
DurableKG returns a fresh cached result or fetches the upstream, stores the
result as a document, and returns it.

## Authoring an endpoint

```clojure
{:kind "endpoint"
 :name "weather"
 :url "https://api/weather"
 :cache-seconds 600
 :forward {:query ["city" "units"] :headers ["accept"] :body false}
 :upstream {:url-template "https://api/weather/{city}" :query ["units"]}
 :cache-key {:query ["city" "units"]}
 :auth {:type "bearer" :secret "WEATHER_TOKEN"}}
```

Mounted at `GET /v1/endpoints/:name`.

## Caching

A cached result is fresh while `now - created_at < cache_seconds`. Results are
stored as documents in the endpoint's collection (default: the
endpoint name), tagged with the cache key. `cache_seconds: 0` disables caching.

## Per-request passthrough

By default an endpoint forwards **nothing** of the caller's request and caches
a single body keyed by the endpoint name. The optional sections opt in:

- `:forward` — an allowlist of query params / headers to forward, and whether
  to forward the body. Anything not named is dropped.
- `:upstream` — how to compose the upstream request. `:url-template` fills
  `{param}` placeholders from forwarded query params; the rest of the forwarded
  query is appended.
- `:cache-key` — which query params / headers vary the cache, so
  `?city=portland` and `?city=tokyo` cache independently. The key is canonical
  (sorted) and stored in document metadata.

`Authorization` and `Cookie` headers are **never** forwarded from the caller,
even if listed.

## Upstream authentication

An endpoint authenticates *itself* to its upstream via `:auth`:

- `{:type "bearer" :secret "NAME"}` — `Authorization: Bearer <secret>`
- `{:type "header" :name "X-Api-Key" :secret "NAME"}`
- `{:type "query" :name "apikey" :secret "NAME"}`

The secret is referenced **by name** and resolved at call time from the
environment variable `DKG_SECRET_<NAME>`. The value never appears in config, the
cache key, the stored document, or logs. A missing secret fails the call (no
unauthenticated upstream request is made); the server warns at boot about any
referenced secret that is unset.

## Calling

```sh
dkg endpoint call weather --query city=portland --header 'accept: application/json'
```

```sh
curl 'localhost:8080/v1/endpoints/weather?city=portland'
```
