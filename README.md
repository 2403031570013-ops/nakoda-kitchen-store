# Nakoda UI

This static app preserves the four supplied Nakoda screens without modifying
their HTML, CSS, JavaScript, or content.

- `/` or `/index.html`: storefront home (`screen-1.html`)
- `/screen-2.html`: product catalog
- `/screen-3.html`: product details
- `/screen-4.html`: cart and checkout

Run locally with:

```text
npm start
```

## Backend

The backend uses Flask and connects to MySQL only on the server. Copy
`backend/.env.example` to `backend/.env`, fill in the local MySQL password,
then verify the database and create the tables:

```text
cd backend
python app.py --check-db
python app.py
```

The local API is available at `http://127.0.0.1:5000/api`. The deployed static
frontend uses `https://nakoda-api.onrender.com` by default; set
`window.NAKODA_API_BASE` before the application scripts if the Render service
URL changes. On Vercel, configure the same URL as `VITE_API_URL` if the
frontend is later migrated to a Vite build (this repository currently serves
static HTML and does not evaluate `import.meta.env`).

## Admin control center

The separate admin application is available at `/admin/login` and is served by
`admin.html`. Admin APIs are protected server-side by the JWT `ADMIN` (or
`SUPER_ADMIN`) role; customer tokens receive `403` responses. To bootstrap an
administrator on a new database, set `ADMIN_EMAIL`, `ADMIN_PASSWORD` and
optionally `ADMIN_NAME` in the backend `.env` before running the database check.
The admin area includes live dashboard KPIs, order workflow/status history,
product/category CRUD, inventory adjustments with reasons and history,
customers, transactions, enquiries, banners, offers, analytics, reports,
notifications, profile and audit logs.

## Customer account

The account center is available at `/account` (and `/login`, `/register`,
`/forgot-password`). Authenticated REST resources cover the dashboard, orders
and tracking, profile, addresses, wishlist, recently viewed products,
transactions and notifications. Run `python app.py --check-db` after pulling
the update so the idempotent schema migrations create the account tables.

Notifications are persisted in MySQL and are created for order placement,
inventory alerts and admin order-status changes. The storefront and admin
headers poll their notification inboxes every 30 seconds. Admins can upload
JPG/PNG/WEBP product images and import validated CSV files (XLSX is supported
when `openpyxl` is installed).
