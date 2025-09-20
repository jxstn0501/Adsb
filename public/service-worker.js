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
  const targetUrl = (event.notification && event.notification.data && event.notification.data.url) || '/';
  event.waitUntil(
    self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(clientList => {
      for (const client of clientList) {
        if (client.url.includes(targetUrl) && 'focus' in client) {
          return client.focus();
        }
      }
      if (self.clients.openWindow) {
        return self.clients.openWindow(targetUrl);
      }
      return null;
    })
  );
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
