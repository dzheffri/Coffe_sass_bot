# Super Admin QR materials

Local implementation only. No production migration is applied automatically.

Apply `migrations/20261023_shop_qr_materials.sql` using the project's reviewed migration process before enabling the editor. It creates new tables/indexes, preserves shops and memberships, and can be repeated. Shop deletion cascades only that physical shop's QR design/events; account deletion is unaffected.

Configuration (server-side only):

- `QR_PUBLIC_BASE_URL`: permanent Web landing origin; defaults to the existing Web deployment, `https://coffee-admin-production-340d.up.railway.app`. A printed QR is `/go/<opaque-token>`. Configure the durable production custom domain before printing if one is available.
- `TELEGRAM_BOT_USERNAME`: existing bot configuration, retaining `forYouMeCoffeBot` as the existing default. The landing opens the existing bot; no Telegram loyalty/start behavior is changed.
- `QR_APP_STORE_URL`: verified real `https://apps.apple.com/...` app URL. Hidden when absent or invalid; no guessed store listing is shipped.
- `QR_GOOGLE_PLAY_URL`: optional real `https://play.google.com/...` Android listing. Hidden while Android is unpublished.

Destinations never come from public query parameters or saved designer fields. Changing them preserves all printed public tokens. No third-party QR service is used.

All editor, image-upload and analytics routes use the same verified actor plus Super Admin allowlist as Community. They accept bearer sessions or verified Telegram MiniApp context. No owner/barista access is granted.

Custom JPEG/PNG images reuse existing validation (8 MiB), atomic randomized persistent storage and the existing `/uploads` static mount, in a separate `/data/uploads/qr` folder. Saving a QR never changes shop profile logo/cover. QR designer URL inputs accept uploaded image paths or that shop's existing canonical profile images; no arbitrary remote images or fetches are introduced.

Public routes expose only active, nondeleted shop name/logo and available choice links. Analytics store event kind, platform, timestamp and material relation. Optional UUID event IDs deduplicate repeated page-open requests. No IP, user identity, raw user-agent or persistent visitor identifier is saved. A transaction/advisory lock enforces a cross-worker cap of 120 events per QR per minute. Reaching the analytics cap never blocks a legitimate destination click: it redirects without adding an event. These are directional scan/click analytics, not unique visitors or proof of a physical scan.

Stats are UTC, rolling 7/30 days or all time, and are isolated by physical shop even inside one network. Conversion is clicks / page opens (multiple choices can yield >100%).
