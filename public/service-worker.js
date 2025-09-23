const CACHE_VERSION = 'v1';
const CACHE_NAME = `heli-tracker-cache-${CACHE_VERSION}`;
const PRECACHE_URLS = ['/', '/index.html', '/manifest.json'];

self.addEventListener('install', event => {
  event.waitUntil(
    (async () => {
      const cache = await caches.open(CACHE_NAME);
      try {
        await cache.addAll(PRECACHE_URLS);
      } catch (error) {
        console.warn('Pre-cache failed', error);
      }
      await self.skipWaiting();
    })()
  );
});

self.addEventListener('activate', event => {
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(
        keys.filter(key => key !== CACHE_NAME).map(key => caches.delete(key))
      );
      await self.clients.claim();
    })()
  );
});

self.addEventListener('fetch', event => {
  const { request } = event;
  if (request.method !== 'GET') {
    return;
  }

  const url = new URL(request.url);

  if (request.mode === 'navigate') {
    event.respondWith(
      (async () => {
        try {
          const networkResponse = await fetch(request);
          const cache = await caches.open(CACHE_NAME);
          await cache.put(request, networkResponse.clone());
          return networkResponse;
        } catch (error) {
          const cache = await caches.open(CACHE_NAME);
          const cachedResponse =
            (await cache.match(request)) ||
            (await cache.match('/index.html')) ||
            (await cache.match('/'));
          if (cachedResponse) {
            return cachedResponse;
          }
          throw error;
        }
      })()
    );
    return;
  }

  if (url.origin === self.location.origin) {
    event.respondWith(
      (async () => {
        const cached = await caches.match(request);
        if (cached) {
          return cached;
        }
        try {
          const networkResponse = await fetch(request);
          const cache = await caches.open(CACHE_NAME);
          await cache.put(request, networkResponse.clone());
          return networkResponse;
        } catch (error) {
          if (cached) {
            return cached;
          }
          throw error;
        }
      })()
    );
  }
});

self.addEventListener('notificationclick', event => {
  event.notification.close();

  const data = (event.notification && event.notification.data) || {};
  const targetUrl = typeof data.url === 'string' && data.url ? data.url : '/';
  const eventPayload = data.event || null;
  const groupKey = data.groupKey || null;

  event.waitUntil((async () => {
    try {
      const clients = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
      let matchedClient = null;
      const target = new URL(targetUrl, self.location.origin);

      for (const client of clients) {
        try {
          const clientUrl = new URL(client.url, self.location.origin);
          if (clientUrl.origin === target.origin) {
            matchedClient = client;
            break;
          }
        } catch (err) {
          // ignore parsing errors for client URLs
        }
      }

      if (matchedClient) {
        if ('focus' in matchedClient) {
          await matchedClient.focus();
        }
        if (eventPayload) {
          matchedClient.postMessage({ type: 'open-event', event: eventPayload, groupKey, url: targetUrl });
        }
        return;
      }

      if (self.clients.openWindow) {
        const opened = await self.clients.openWindow(targetUrl);
        if (opened && eventPayload) {
          opened.postMessage({ type: 'open-event', event: eventPayload, groupKey, url: targetUrl });
        }
      }
    } catch (err) {
      console.warn('notificationclick handler failed', err);
      if (self.clients && self.clients.openWindow) {
        try {
          await self.clients.openWindow(targetUrl);
        } catch (openErr) {
          console.warn('fallback openWindow failed', openErr);
        }
      }
    }
  })());
});

self.addEventListener('push', event => {
  let payload = {};
  try {
    payload = event.data ? event.data.json() : {};
  } catch (err) {
    payload = { body: event.data ? event.data.text() : '' };
  }

  const title = payload.title || 'Heli Tracker';
  const options = Object.assign(
    {
      body: payload.body || 'Neues Ereignis verfügbar.',
      icon: payload.icon || '/icons/icon-192.png',
      badge: payload.badge || '/icons/icon-192.png',
      data: payload.data || { url: '/' },
      tag: payload.tag || `heli-tracker-${Date.now()}`
    },
    payload.options || {}
  );

  event.waitUntil(self.registration.showNotification(title, options));
});
