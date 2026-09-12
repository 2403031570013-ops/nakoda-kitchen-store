import csv
import io
import json
import os
import re
import hashlib
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import jwt
import mysql.connector
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from flask_socketio import SocketIO, emit, join_room, leave_room
from mysql.connector import Error
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

load_dotenv(Path(__file__).with_name(".env"))

BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = BASE_DIR / "backend" / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(BASE_DIR / "static"),
)
app.config["SECRET_KEY"] = os.getenv("JWT_SECRET", "nakoda-development-secret-key-please-change")
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "https://nakoda-kitchen-store.vercel.app").rstrip("/")
ALLOWED_ORIGINS = {
    FRONTEND_ORIGIN,
    "http://localhost:3000",
    "http://127.0.0.1:3000",
}
socketio = SocketIO(app, cors_allowed_origins=list(ALLOWED_ORIGINS), async_mode="threading")
SOCKET_USERS = {}


@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        return ("", 204)


@app.after_request
def add_cors_headers(response):
    origin = request.headers.get("Origin")
    if origin in ALLOWED_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers.add("Vary", "Origin")
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    return response


def database_config():
    values = {
        "host": os.getenv("DB_HOST", "localhost").strip(),
        "port": os.getenv("DB_PORT", "3306").strip(),
        "database": os.getenv("DB_NAME", "nakoda_db").strip(),
        "user": os.getenv("DB_USERNAME", "root").strip(),
        "password": os.getenv("DB_PASSWORD", ""),
    }
    required = {
        "DB_HOST": values["host"],
        "DB_NAME": values["database"],
        "DB_USERNAME": values["user"],
        "DB_PASSWORD": values["password"],
    }
    if os.getenv("RENDER") or os.getenv("RENDER_SERVICE_ID"):
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise RuntimeError(
                "Missing required database environment variable(s): "
                + ", ".join(missing)
                + ". Configure the hosted MySQL connection in Render Environment Variables."
            )
    try:
        port = int(values["port"])
    except ValueError as exc:
        raise RuntimeError("DB_PORT must be a valid integer.") from exc
    if not 1 <= port <= 65535:
        raise RuntimeError("DB_PORT must be between 1 and 65535.")
    return {
        "host": values["host"],
        "port": port,
        "database": values["database"],
        "user": values["user"],
        "password": values["password"],
        "autocommit": False,
        "connection_timeout": int(os.getenv("DB_CONNECTION_TIMEOUT", "10")),
    }


def connect(database=True):
    config = database_config()
    if not database:
        config.pop("database")
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except mysql.connector.Error as exc:
        message = str(exc).lower()
        if "name or service not known" in message or "nodename nor servname" in message:
            detail = "MySQL hostname could not be resolved. Check DB_HOST in Render."
        elif "access denied" in message:
            detail = "MySQL authentication failed. Check DB_USERNAME and DB_PASSWORD."
        elif "unknown database" in message:
            detail = "MySQL database was not found. Check DB_NAME."
        elif "timed out" in message or "can't connect" in message:
            detail = "Could not connect to MySQL server. Check DB_HOST and DB_PORT."
        else:
            detail = (
                "MySQL connection failed. Verify DB_HOST, DB_PORT, database name, "
                "username, password, and network access."
            )
        raise RuntimeError(detail) from exc


def initialize_database():
    connection = connect(database=False)
    try:
        cursor = connection.cursor()
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS `{database_config()['database']}` "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        cursor.close()
        connection.commit()
    finally:
        connection.close()

    connection = connect()
    try:
        cursor = connection.cursor()
        schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
        for statement in schema.split(";"):
            statement = statement.strip()
            if statement:
                cursor.execute(statement)
        connection.commit()
        cursor.close()
    finally:
        connection.close()

    # Apply compatibility migrations before seed inserts reference upgraded columns.
    ensure_schema_migrations()
    ensure_seed_data()
    print("MySQL database connection established successfully.")
    ensure_relevant_product_images()


def ensure_schema_migrations():
    """Keep installations created from the original schema upgradeable."""
    connection = connect()
    migrations = (
        "ALTER TABLE users ADD COLUMN role VARCHAR(30) NOT NULL DEFAULT 'CUSTOMER'",
        "ALTER TABLE users ADD COLUMN profile_image_url TEXT",
        "ALTER TABLE orders ADD COLUMN payment_method VARCHAR(40) NOT NULL DEFAULT 'COD'",
        "ALTER TABLE orders ADD COLUMN payment_status VARCHAR(40) NOT NULL DEFAULT 'PENDING'",
        "ALTER TABLE orders ADD COLUMN subtotal DECIMAL(10,2) NOT NULL DEFAULT 0",
        "ALTER TABLE orders ADD COLUMN delivery_charge DECIMAL(10,2) NOT NULL DEFAULT 0",
        "ALTER TABLE orders ADD COLUMN discount DECIMAL(10,2) NOT NULL DEFAULT 0",
        "ALTER TABLE orders ADD COLUMN expected_delivery DATE NULL",
        "ALTER TABLE users ADD COLUMN last_login DATETIME NULL",
        "ALTER TABLE users ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT TRUE",
        "ALTER TABLE products ADD COLUMN sku VARCHAR(255) NULL",
        "ALTER TABLE products ADD COLUMN short_description TEXT NULL",
        "ALTER TABLE products ADD COLUMN brand VARCHAR(120) NULL",
        "ALTER TABLE products ADD COLUMN subcategory VARCHAR(120) NULL",
        "ALTER TABLE products ADD COLUMN low_stock_threshold INT NOT NULL DEFAULT 5",
        "ALTER TABLE products ADD COLUMN weight VARCHAR(80) NULL",
        "ALTER TABLE products ADD COLUMN dimensions VARCHAR(120) NULL",
        "ALTER TABLE products ADD COLUMN material VARCHAR(120) NULL",
        "ALTER TABLE products ADD COLUMN color VARCHAR(80) NULL",
        "ALTER TABLE products ADD COLUMN size VARCHAR(80) NULL",
        "ALTER TABLE products ADD COLUMN capacity VARCHAR(80) NULL",
        "ALTER TABLE products ADD COLUMN tags TEXT NULL",
        "ALTER TABLE products ADD COLUMN is_featured BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE products ADD COLUMN is_bestseller BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE products ADD COLUMN is_new_arrival BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE products ADD COLUMN is_trending BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE products ADD COLUMN updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "ALTER TABLE banners ADD COLUMN subtitle VARCHAR(255) NULL",
        "ALTER TABLE banners ADD COLUMN banner_type VARCHAR(40) NOT NULL DEFAULT 'PROMOTIONAL'",
        "ALTER TABLE banners ADD COLUMN cta_text VARCHAR(80) NULL",
        "ALTER TABLE banners ADD COLUMN target_type VARCHAR(40) NULL",
        "ALTER TABLE banners ADD COLUMN priority INT NOT NULL DEFAULT 0",
        "ALTER TABLE banners ADD COLUMN start_date DATETIME NULL",
        "ALTER TABLE banners ADD COLUMN end_date DATETIME NULL",
        "ALTER TABLE notifications ADD COLUMN order_number VARCHAR(32) NULL",
        "ALTER TABLE notifications ADD COLUMN metadata TEXT NULL",
        "ALTER TABLE notifications ADD INDEX notification_order (order_id, created_at)",
        "ALTER TABLE transactions ADD COLUMN utr VARCHAR(100) NULL",
        "ALTER TABLE transactions ADD COLUMN payment_note TEXT NULL",
        "ALTER TABLE transactions ADD COLUMN verified_by BIGINT NULL",
        "ALTER TABLE transactions ADD COLUMN verified_at DATETIME NULL",
        "ALTER TABLE transactions ADD COLUMN rejection_reason VARCHAR(255) NULL",
        "ALTER TABLE transactions ADD INDEX transaction_utr (utr)",
        "ALTER TABLE categories ADD COLUMN parent_id BIGINT NULL",
        "ALTER TABLE categories ADD COLUMN icon VARCHAR(40) NULL",
        "ALTER TABLE products ADD COLUMN badge_label VARCHAR(80) NULL",
        "ALTER TABLE products ADD COLUMN badge_color VARCHAR(20) NULL",
        """CREATE TABLE IF NOT EXISTS campaigns (
            id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(180) NOT NULL,
            slug VARCHAR(200) NOT NULL UNIQUE, description TEXT,
            campaign_type VARCHAR(40) NOT NULL DEFAULT 'PROMOTION',
            banner_image_url TEXT, landing_url TEXT, start_date DATETIME NULL,
            end_date DATETIME NULL, status VARCHAR(30) NOT NULL DEFAULT 'DRAFT',
            budget DECIMAL(12,2) NULL, created_by BIGINT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX campaign_active (status, start_date, end_date)
        )""",
        """CREATE TABLE IF NOT EXISTS campaign_products (
            campaign_id BIGINT NOT NULL, product_id BIGINT NOT NULL,
            PRIMARY KEY (campaign_id, product_id),
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
        )""",
        """CREATE TABLE IF NOT EXISTS product_images (
            id BIGINT PRIMARY KEY AUTO_INCREMENT, product_id BIGINT NOT NULL,
            image_url TEXT NOT NULL, alt_text VARCHAR(255), sort_order INT NOT NULL DEFAULT 0,
            is_primary BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
            INDEX product_images_order (product_id, sort_order)
        )""",
        """INSERT INTO product_images (product_id, image_url, is_primary, sort_order)
           SELECT p.id, p.image_url, 1, 0 FROM products p
           WHERE p.image_url IS NOT NULL AND p.image_url <> ''
             AND NOT EXISTS (SELECT 1 FROM product_images pi WHERE pi.product_id=p.id)""",
        """CREATE TABLE IF NOT EXISTS support_conversations (
            id BIGINT PRIMARY KEY AUTO_INCREMENT, customer_id BIGINT NOT NULL,
            subject VARCHAR(180) NOT NULL, reason VARCHAR(80), order_id BIGINT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            closed_at DATETIME NULL, FOREIGN KEY (customer_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE SET NULL,
            INDEX support_conversation_customer (customer_id, updated_at),
            INDEX support_conversation_status (status, updated_at)
        )""",
        """CREATE TABLE IF NOT EXISTS support_messages (
            id BIGINT PRIMARY KEY AUTO_INCREMENT, conversation_id BIGINT NOT NULL,
            sender_type VARCHAR(20) NOT NULL, sender_id BIGINT NOT NULL, body TEXT NOT NULL,
            is_read BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (conversation_id) REFERENCES support_conversations(id) ON DELETE CASCADE,
            FOREIGN KEY (sender_id) REFERENCES users(id) ON DELETE CASCADE,
            INDEX support_message_conversation (conversation_id, created_at),
            INDEX support_message_unread (conversation_id, sender_type, is_read)
        )""",
    )
    try:
        cursor = connection.cursor()
        for migration in migrations:
            try:
                cursor.execute(migration)
            except Error as exc:
                # MySQL reports duplicate-column errors for an already upgraded DB.
                if getattr(exc, "errno", None) not in (1060, 1061, 1826):
                    raise
        cursor.execute("UPDATE users SET role = 'CUSTOMER' WHERE role IS NULL OR role = ''")
        connection.commit()
    finally:
        connection.close()


def ensure_relevant_product_images():
    """Replace the old repeated seed photos with category-appropriate images."""
    image_pools = {
        "kitchen-appliances": [
            "photo-1585515320310-259814833e62", "photo-1556910103-1c02745aae4d",
            "photo-1571175443880-49eec7d7b7a1", "photo-1585515320310-259814833e62",
            "photo-1556911220-e15b29be8c8f", "photo-1594221708779-94832f4320d1",
        ],
        "cookware": [
            "photo-1556910103-1c02745aae4d", "photo-1584990347449-ae5d6f9a9d48",
            "photo-1556911220-bff31c812dba", "photo-1590794056226-79ef3a8147e1",
            "photo-1584990347449-ae5d6f9a9d48", "photo-1515003197210-e0cd71810b5f",
        ],
        "kitchen-tools": [
            "photo-1556911220-e15b29be8c8f", "photo-1593618998160-e34014e67546",
            "photo-1556910103-1c02745aae4d", "photo-1583778176476-4a8b02a64c01",
            "photo-1556911220-bff31c812dba", "photo-1594385208974-2e75f8d7b3a3",
        ],
        "storage": [
            "photo-1583947215259-38e31be8751f", "photo-1610701596007-11502861dcfa",
            "photo-1600566753190-17f0baa2a6c3", "photo-1586023492125-27b2c045efd7",
            "photo-1558997519-83ea9252edf8", "photo-1595428774223-ef52624120d2",
            "photo-1600566753086-00f18fb6b3ea", "photo-1616486338812-3dadae4b4ace",
            "photo-1600607687920-4e2a09cf159d", "photo-1600566753190-17f0baa2a6c3",
        ],
        "dining": [
            "photo-1603199506016-b9a594b593c0", "photo-1515003197210-e0cd71810b5f",
            "photo-1547592180-85f173990554", "photo-1495474472287-4d71bcdd2085",
            "photo-1578985545062-69928b1d9587", "photo-1601050690597-df0568f70950",
            "photo-1603199506016-b9a594b593c0", "photo-1559339352-11d035aa65de",
            "photo-1552566626-52f8b828add9", "photo-1517248135467-4c7edcad34c4",
        ],
        "plastic-utility": [
            "photo-1584622650111-993a426fbf0a", "photo-1558618666-fcd25c85cd64",
            "photo-1581578731548-c64695cc6952", "photo-1584622650111-993a426fbf0a",
            "photo-1594620302200-9a762244a156", "photo-1584622650111-993a426fbf0a",
            "photo-1600566753190-17f0baa2a6c3", "photo-1583947215259-38e31be8751f",
        ],
        "cleaning": [
            "photo-1581578731548-c64695cc6952", "photo-1558618666-fcd25c85cd64",
            "photo-1527515637462-cff94eecc1ac", "photo-1585421514738-01798e348b17",
            "photo-1563453392212-326f5e854473", "photo-1584820927498-cfe5211fd8bf",
        ],
        "household-utility": [
            "photo-1558997519-83ea9252edf8", "photo-1586023492125-27b2c045efd7",
            "photo-1595428774223-ef52624120d2", "photo-1616486338812-3dadae4b4ace",
            "photo-1600566753086-00f18fb6b3ea", "photo-1600607687939-ce8a6c25118c",
        ],
    }
    keyword_images = {
        "baati oven": "photo-1585515320310-259814833e62",
        "oven": "photo-1585515320310-259814833e62",
        "air fryer": "photo-1556910103-1c02745aae4d",
        "blender": "photo-1571175443880-49eec7d7b7a1",
        "kadai": "photo-1517433670267-08bbd4be890f",
        "frying pan": "photo-1590794056226-79ef3a8147e1",
        "pressure cooker": "photo-1556911220-e15b29be8c8f",
        "mop": "photo-1581578731548-c64695cc6952",
        "dinner set": "photo-1603199506016-b9a594b593c0",
        "mug": "photo-1495474472287-4d71bcdd2085",
        "jar": "photo-1610701596007-11502861dcfa",
        "organizer": "photo-1595428774223-ef52624120d2",
    }
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT setting_value FROM settings WHERE setting_key='product_images_relevant_v4'")
        if cursor.fetchone():
            return
        cursor.execute(
            "SELECT p.id, p.name, c.slug AS category_slug FROM products p "
            "LEFT JOIN categories c ON c.id=p.category_id ORDER BY p.id"
        )
        for product in cursor.fetchall():
            name = str(product["name"] or "").lower()
            image_id = next(
                (image for keyword, image in keyword_images.items() if keyword in name),
                None,
            )
            if not image_id:
                pool = image_pools.get(product["category_slug"], image_pools["household-utility"])
                image_id = pool[(product["id"] - 1) % len(pool)]
            image_url = f"https://images.unsplash.com/{image_id}?auto=format&fit=crop&w=900&q=85"
            cursor.execute("UPDATE products SET image_url=%s WHERE id=%s", (image_url, product["id"]))
            cursor.execute(
                "UPDATE product_images SET image_url=%s, alt_text=%s "
                "WHERE product_id=%s AND is_primary=1",
                (image_url, product["name"], product["id"]),
            )
        cursor.execute(
            "INSERT INTO settings (setting_key, setting_value) VALUES "
            "('product_images_relevant_v4', 'true') "
            "ON DUPLICATE KEY UPDATE setting_value=VALUES(setting_value)"
        )
        connection.commit()
    finally:
        connection.close()


def ensure_seed_data():
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        categories = [
            ("Kitchen Appliances", "kitchen-appliances", "Practical appliances for faster everyday cooking"),
            ("Cookware", "cookware", "Everyday pots, pans and pressure cookers"),
            ("Kitchen Tools", "kitchen-tools", "Useful tools for prep and serving"),
            ("Storage", "storage", "Smart storage for kitchens and homes"),
            ("Dining", "dining", "Tableware for everyday meals and hosting"),
            ("Plastic & Utility", "plastic-utility", "Durable household utility essentials"),
            ("Cleaning", "cleaning", "Cleaning tools for a fresher home"),
            ("Household Utility", "household-utility", "Daily-use home organization products"),
        ]
        for name, slug, desc in categories:
            cursor.execute(
                "INSERT IGNORE INTO categories (name, slug, description, is_active) VALUES (%s, %s, %s, TRUE)",
                (name, slug, desc),
            )
        cursor.execute("SELECT id, slug FROM categories")
        category_ids = {row["slug"]: row["id"] for row in cursor.fetchall()}
        cursor.execute("SELECT COUNT(*) AS total FROM products")
        if cursor.fetchone()["total"] == 0:
            product_rows = [
                (category_ids["kitchen-appliances"], "Nakoda Tandoor Oven", "nakoda-tandoor-oven", "Heavy-duty clay inspired oven for authentic cooking", 3799, 4499, "https://images.unsplash.com/photo-1582719478250-c89cae4dc85b?auto=format&fit=crop&w=900&q=80", 22),
                (category_ids["cookware"], "Copper Finish Kadai Set", "copper-finish-kadai-set", "Premium cookware for daily family meals", 2199, 2899, "https://images.unsplash.com/photo-1517433670267-08bbd4be890f?auto=format&fit=crop&w=900&q=80", 18),
                (category_ids["kitchen-appliances"], "Smart Air Fryer", "smart-air-fryer", "Fast and healthy cooking with smart presets", 4599, 5299, "https://images.unsplash.com/photo-1556910103-1c02745aae4d?auto=format&fit=crop&w=900&q=80", 14),
                (category_ids["kitchen-appliances"], "Mini Blender Pro", "mini-blender-pro", "Compact blending for smoothies and sauces", 1999, 2499, "https://images.unsplash.com/photo-1571175443880-49eec7d7b7a1?auto=format&fit=crop&w=900&q=80", 31),
                (category_ids["dining"], "Handcrafted Dining Set", "handcrafted-dining-set", "Modern dining set with premium matte finish", 6899, 7999, "https://images.unsplash.com/photo-1505693416388-ac5ce068fe85?auto=format&fit=crop&w=900&q=80", 10),
                (category_ids["storage"], "Modular Kitchen Rack", "modular-kitchen-rack", "Smart storage for small kitchens and counters", 2499, 3199, "https://images.unsplash.com/photo-1484154218962-a197022b5858?auto=format&fit=crop&w=900&q=80", 27),
                (category_ids["dining"], "Terracotta Decor Bowl", "terracotta-decor-bowl", "Artisan-inspired serving bowl with warm detailing", 1299, 1699, "https://images.unsplash.com/photo-1495474472287-4d71bcdd2085?auto=format&fit=crop&w=900&q=80", 36),
            ]
            for category_id, name, slug, description, price, compare_at_price, image_url, stock_quantity in product_rows:
                cursor.execute(
                    "INSERT INTO products (category_id, name, slug, description, price, compare_at_price, image_url, stock_quantity, is_active) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE)",
                    (category_id, name, slug, description, price, compare_at_price, image_url, stock_quantity),
                )
        cursor.execute("SELECT setting_value FROM settings WHERE setting_key = 'catalog_seeded'")
        catalog_seeded = cursor.fetchone()
        if not catalog_seeded:
            catalog = {
                "kitchen-appliances": [
                    "Air Fryer 4L Digital", "Air Fryer 6L Family", "OTG Oven 28L", "OTG Oven 42L",
                    "Microwave Oven 20L", "Electric Kettle 1.5L", "Mixer Grinder 500W", "Mixer Grinder 750W",
                    "Hand Blender 300W", "Juicer Mixer 500W", "Citrus Juicer Compact", "Cold Press Juicer",
                    "Food Processor 800W", "Electric Chopper 500ml", "Sandwich Maker Grill", "Pop-up Toaster 2 Slice",
                    "Electric Rice Cooker 1.8L", "Multi Cooker 5L", "Induction Cooktop 2000W", "Electric Stove Double",
                    "Infrared Cooktop", "Electric Tandoor 16L", "Smokeless Electric Grill", "Waffle Maker Classic",
                    "Pancake Maker", "Dosa Maker", "Idli Maker 12 Cavity", "Baati Oven", "Egg Boiler 7 Egg",
                    "Drip Coffee Maker", "Milk Frother", "Hand Mixer 250W", "Stand Mixer 5L", "Electric Whisk",
                    "Electric Pressure Cooker 5L", "Slow Cooker 3.5L", "Steam Cooker", "Soup Maker",
                    "Popcorn Maker", "Bread Maker", "Digital Kitchen Scale", "Digital Kitchen Timer",
                    "Water Purifier Tap Kit", "Chimney Filter Accessory", "Toaster Oven Compact",
                    "Portable Induction Mini", "Vegetable Chopper Pull Cord", "Hot Plate Single",
                    "Sandwich Maker Panini", "Coffee Grinder", "Food Dehydrator", "Electric Spice Grinder",
                    "Yogurt Maker", "Portable Ice Maker", "Kitchen Exhaust Filter",
                ],
                "cookware": [
                    "Hard Anodized Pressure Cooker 3L", "Stainless Steel Pressure Cooker 5L",
                    "Non-stick Kadai 2.5L", "Granite Frying Pan 24cm", "Induction Tawa 28cm",
                    "Dosa Tawa 30cm", "Roti Tawa 26cm", "Triply Saucepan 1.5L", "Milk Pan 1L",
                    "Stainless Handi 3L", "Biryani Handi 5L", "Casserole Set 3 Piece",
                    "Cookware Set 5 Piece", "Cast Iron Dutch Oven", "Stainless Stockpot 8L",
                    "Non-stick Appam Pan", "Paniyaram Pan", "Granite Saucepan", "Triply Kadai 3L",
                ],
                "kitchen-tools": [
                    "Stainless Serving Spoon Set", "Silicone Ladle", "Nylon Turner Set", "Bamboo Spatula",
                    "Balloon Whisk", "Kitchen Tongs", "Julienne Peeler", "Box Grater", "Steel Strainer",
                    "Fine Mesh Sieve", "Stainless Colander", "Chef Knife 8 Inch", "Knife Set 6 Piece",
                    "Kitchen Scissors", "Wooden Rolling Pin", "Chakla Belan Set", "Potato Masher",
                    "Measuring Cup Set", "Measuring Spoon Set", "Glass Oil Dispenser", "Garlic Press",
                    "Steel Lemon Squeezer", "Bottle Opener", "Manual Can Opener", "Silicone Ice Tray",
                ],
                "storage": [
                    "Airtight Container Set 5 Piece", "Glass Storage Jar Set", "Stainless Spice Box",
                    "Masala Box 7 Compartment", "Oil Container 2L", "Rice Storage Container 10kg",
                    "Flour Storage Container 5kg", "Dal Container Set", "Lunch Box 3 Compartment",
                    "Insulated Tiffin Box", "Steel Water Bottle 1L", "Fridge Container Set",
                    "Fridge Organizer Tray", "Drawer Cutlery Organizer", "Kitchen Organizer Rack",
                    "Storage Basket Medium", "Stackable Storage Box", "Pantry Canister Set",
                ],
                "dining": [
                    "Dinner Set 18 Piece", "Ceramic Dinner Plate Set", "Stoneware Bowl Set",
                    "Serving Bowl Large", "Borosilicate Glass Set", "Coffee Mug Set", "Tea Cup Set",
                    "Ceramic Tea Set", "Coffee Cups with Saucers", "Stainless Serving Tray",
                    "Insulated Serving Casserole", "Cutlery Set 24 Piece", "Water Jug 2L", "Glass Pitcher",
                ],
                "plastic-utility": [
                    "Plastic Bucket 20L", "Plastic Mug Set", "Laundry Tub Large", "Bathroom Basin",
                    "Laundry Basket", "Stackable Utility Basket", "Plastic Basket Set", "Swing Dustbin",
                    "Pedal Bin 12L", "Plastic Storage Box", "Food Storage Container Set", "Fridge Container Set",
                    "Drawer Organizer Set", "Plastic Stool", "Plastic Serving Tray", "Plastic Plate Set",
                    "Plastic Bowl Set", "Plastic Cup Set", "Plastic Jar Set", "Water Bottle 1L",
                    "Plastic Lunch Box", "Bathroom Organizer", "Multipurpose Box", "Clothes Hanger Set",
                ],
                "cleaning": [
                    "Microfiber Spin Mop", "Floor Wiper", "Angled Grass Broom", "Dustpan and Brush Set",
                    "Cleaning Brush Set", "Dishwashing Brush", "Window Squeegee", "Cleaning Bucket Set",
                    "Microfiber Cloth Pack", "Kitchen Scrubber Set", "Bathroom Cleaning Brush",
                    "Garbage Bags Roll", "Pedal Dustbin", "Drain Cleaning Brush",
                ],
                "household-utility": [
                    "Foldable Umbrella", "Metal Clothes Hanger Set", "Laundry Clips Pack", "Shoe Organizer",
                    "Bathroom Utility Shelf", "Home Organization Box", "Travel Storage Pouch",
                    "Reusable Shopping Bag", "Door Hook Set", "Ironing Mat", "Mosquito Net",
                    "Kitchen Step Stool", "Multipurpose Utility Rack", "Seasonal Storage Bag",
                ],
            }
            image_pool = [
                "photo-1556910103-1c02745aae4d", "photo-1582719478250-c89cae4dc85b",
                "photo-1517433670267-08bbd4be890f", "photo-1571175443880-49eec7d7b7a1",
                "photo-1505693416388-ac5ce068fe85", "photo-1484154218962-a197022b5858",
                "photo-1495474472287-4d71bcdd2085", "photo-1556911220-e15b29be8c8f",
            ]
            brand_pool = ["Prestige", "Pigeon", "Bajaj", "Philips", "Havells", "Usha", "Butterfly", "Wonderchef", "Milton", "Cello", "Local Choice"]
            seed_index = 0
            for category_slug, names in catalog.items():
                for name in names:
                    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
                    cursor.execute("SELECT id FROM products WHERE slug=%s", (slug,))
                    if cursor.fetchone():
                        continue
                    price = 499 + (seed_index % 12) * 275 + (len(name) % 5) * 60
                    mrp = price + 250 + (seed_index % 4) * 100
                    image = f"https://images.unsplash.com/{image_pool[seed_index % len(image_pool)]}?auto=format&fit=crop&w=900&q=80"
                    cursor.execute(
                        "INSERT INTO products (category_id,name,slug,sku,brand,subcategory,short_description,description,"
                        "price,compare_at_price,image_url,stock_quantity,low_stock_threshold,tags,is_active) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,5,%s,TRUE)",
                        (category_ids[category_slug], name, slug, f"NKO-{seed_index + 1000:05d}",
                         brand_pool[seed_index % len(brand_pool)], category_slug.replace("-", " ").title(),
                         f"{name} for dependable everyday use.", f"{name} with practical design and durable materials.",
                         price, mrp, image, 8 + seed_index % 45, category_slug.replace("-", ", ")),
                    )
                    seed_index += 1
            cursor.execute(
                "INSERT INTO settings (setting_key, setting_value) VALUES ('catalog_seeded', 'true') "
                "ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value)"
            )
        cursor.execute("SELECT setting_value FROM settings WHERE setting_key = 'catalog_seeded_v2'")
        if not cursor.fetchone():
            extra_products = [
                ("kitchen-appliances", "Digital Air Fryer 5.5L", 3299, 4299),
                ("kitchen-appliances", "OTG Oven 19L Compact", 2899, 3699),
                ("kitchen-appliances", "Electric Kettle 2L Steel", 1199, 1599),
                ("kitchen-appliances", "Mixer Grinder 600W 3 Jar", 2499, 3299),
                ("kitchen-appliances", "Portable Juicer USB", 899, 1299),
                ("kitchen-appliances", "Electric Chopper 1L Glass Bowl", 1399, 1899),
                ("kitchen-appliances", "Rice Cooker 1.8L Automatic", 1999, 2599),
                ("kitchen-appliances", "Sandwich Maker 4 Slice", 1699, 2299),
                ("cookware", "Stainless Steel Kadai 3L", 1599, 2199),
                ("cookware", "Non-stick Fry Pan 28cm", 999, 1499),
                ("cookware", "Granite Tawa 30cm", 1299, 1799),
                ("cookware", "Hard Anodized Handi 4L", 2199, 2899),
                ("cookware", "Stainless Saucepan with Lid", 1199, 1699),
                ("cookware", "Cast Iron Skillet 10 Inch", 1899, 2499),
                ("cookware", "Induction Cookware Set 3 Piece", 2799, 3699),
                ("cookware", "Copper Bottom Tope Set", 1499, 2099),
                ("kitchen-tools", "Wooden Kitchen Tool Set 6 Piece", 799, 1199),
                ("kitchen-tools", "Silicone Spatula Set 5 Piece", 499, 799),
                ("kitchen-tools", "Stainless Steel Chopping Board", 899, 1299),
                ("kitchen-tools", "Rotary Vegetable Slicer", 1299, 1799),
                ("kitchen-tools", "Kitchen Scale Digital 10kg", 699, 999),
                ("kitchen-tools", "Mandoline Slicer Safety Guard", 1099, 1499),
                ("kitchen-tools", "Rolling Pin Marble", 599, 899),
                ("kitchen-tools", "Measuring Jug Set 3 Piece", 649, 999),
                ("storage", "Airtight Container Set 10 Piece", 1599, 2199),
                ("storage", "Bamboo Cutlery Organizer", 999, 1399),
                ("storage", "Stackable Spice Jar Set 12 Piece", 1199, 1699),
                ("storage", "Steel Lunch Box 3 Tier", 899, 1299),
                ("storage", "Insulated Water Bottle 750ml", 699, 1099),
                ("storage", "Rotating Kitchen Organizer", 799, 1199),
                ("storage", "Vacuum Storage Bags 8 Piece", 599, 899),
                ("storage", "Wall Mounted Utensil Rack", 1299, 1799),
                ("dining", "Porcelain Dinner Set 12 Piece", 2499, 3299),
                ("dining", "Ceramic Coffee Mug Pair", 699, 999),
                ("dining", "Bamboo Serving Tray Large", 899, 1299),
                ("dining", "Stainless Steel Dinner Plate Set", 1799, 2399),
                ("dining", "Glass Tumbler Set 6 Piece", 799, 1199),
                ("dining", "Ceramic Serving Platter", 999, 1499),
                ("plastic-utility", "Multipurpose Storage Basket Large", 599, 899),
                ("plastic-utility", "Food Storage Box Set 7 Piece", 899, 1299),
                ("plastic-utility", "Bathroom Corner Shelf", 799, 1199),
                ("plastic-utility", "Laundry Basket with Lid", 1199, 1699),
                ("cleaning", "Spin Mop with Bucket", 1499, 2199),
                ("cleaning", "Floor Cleaning Wiper Premium", 399, 599),
                ("cleaning", "Dishwash Scrub Pad Set", 249, 399),
                ("cleaning", "Microfiber Kitchen Towel Set", 349, 549),
                ("household-utility", "Foldable Step Stool", 899, 1299),
                ("household-utility", "Multipurpose Hanger Set 10 Piece", 499, 799),
                ("household-utility", "Door Storage Hook Set", 299, 499),
                ("household-utility", "Reusable Shopping Bag Set", 399, 599),
            ]
            extra_images = [
                "photo-1556910103-1c02745aae4d",
                "photo-1582719478250-c89cae4dc85b",
                "photo-1517433670267-08bbd4be890f",
                "photo-1571175443880-49eec7d7b7a1",
                "photo-1505693416388-ac5ce068fe85",
                "photo-1484154218962-a197022b5858",
                "photo-1495474472287-4d71bcdd2085",
                "photo-1556911220-e15b29be8c8f",
            ]
            for index, (category_slug, name, price, compare_at_price) in enumerate(extra_products):
                slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
                cursor.execute("SELECT id FROM products WHERE slug=%s", (slug,))
                if cursor.fetchone():
                    continue
                image = f"https://images.unsplash.com/{extra_images[index % len(extra_images)]}?auto=format&fit=crop&w=900&q=80"
                cursor.execute(
                    "INSERT INTO products (category_id,name,slug,sku,brand,subcategory,short_description,description,"
                    "price,compare_at_price,image_url,stock_quantity,low_stock_threshold,tags,is_active) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,5,%s,TRUE)",
                    (
                        category_ids[category_slug],
                        name,
                        slug,
                        f"NKO-V2-{index + 1:03d}",
                        "Nakoda Select",
                        category_slug.replace("-", " ").title(),
                        f"{name} for dependable everyday use.",
                        f"{name} with practical design and durable materials.",
                        price,
                        compare_at_price,
                        image,
                        12 + index % 30,
                        category_slug.replace("-", ", "),
                    ),
                )
            cursor.execute(
                "INSERT INTO settings (setting_key, setting_value) VALUES ('catalog_seeded_v2', 'true') "
                "ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value)"
            )
        cursor.execute("SELECT setting_value FROM settings WHERE setting_key = 'catalog_seeded_v3'")
        if not cursor.fetchone():
            catalogue_expansion = {
                "kitchen-appliances": [
                    "Electric Chai Maker 1.5L", "Electric Handheld Citrus Press",
                    "Digital Egg Boiler 14 Egg", "Rechargeable Milk Frother",
                    "Countertop Vegetable Steamer", "Electric Roti Maker",
                    "Portable Coffee Brewer", "Automatic Chapati Warmer",
                    "Electric Spice Mill Set", "Mini Food Sealer",
                ],
                "cookware": [
                    "Tri Ply Frying Pan 26cm", "Ceramic Coated Kadai 2L",
                    "Stainless Steel Sauce Pot 2L", "Non Stick Paniyaram Pan",
                    "Cast Iron Roti Tawa 28cm", "Aluminium Biriyani Handi 4L",
                    "Granite Cookpot with Lid", "Stainless Steel Steamer 3 Tier",
                    "Copper Bottom Milk Pan", "Hard Anodized Grill Pan",
                ],
                "kitchen-tools": [
                    "Adjustable Measuring Spoon", "Stainless Steel Potato Ricer",
                    "Bamboo Salad Server Set", "Rotary Cheese Grater",
                    "Silicone Kitchen Brush", "Vegetable Julienne Cutter",
                    "Stainless Steel Dough Scraper", "Manual Noodle Maker",
                    "Kitchen Herb Scissors", "Oil Sprayer Bottle",
                ],
                "storage": [
                    "Airtight Grain Dispenser 5kg", "Bamboo Bread Box",
                    "Modular Cabinet Organizer", "Stackable Vegetable Basket",
                    "Spice Drawer Organizer", "Steel Flour Container 8kg",
                    "Hanging Fridge Organizer", "Rotating Bottle Rack",
                    "Glass Pickle Jar Set", "Collapsible Food Container Set",
                ],
                "dining": [
                    "Opal Glass Dinner Set 16 Piece", "Acacia Wood Serving Board",
                    "Insulated Soup Bowl Set", "Ceramic Ramen Bowl Pair",
                    "Stainless Steel Snack Plate Set", "Borosilicate Tea Glass Set",
                    "Bamboo Cutlery Holder", "Porcelain Gravy Boat",
                    "Glass Oil And Vinegar Set", "Steel Breakfast Plate Set",
                ],
                "plastic-utility": [
                    "Dustproof Shoe Storage Box", "Foldable Plastic Crate",
                    "Leakproof Water Can 10L", "Multipurpose Drawer Cabinet",
                    "Plastic Laundry Hanger Rack", "Food Grade Mixing Bowl Set",
                    "Bathroom Stool with Handle", "Plastic Cleaning Caddy",
                    "Stackable Clothes Storage Bin", "Reusable Produce Bag Set",
                ],
                "cleaning": [
                    "Flat Spray Mop with Refill", "Long Handle Dusting Brush",
                    "Reusable Lint Roller", "Stainless Sink Scrubber",
                    "Toilet Cleaning Brush with Holder", "Kitchen Degreasing Brush",
                    "Reusable Cleaning Gloves", "Ceiling Fan Duster",
                    "Drain Hair Catcher Set", "Dish Drying Mat",
                ],
                "household-utility": [
                    "Foldable Clothes Drying Stand", "Wall Mounted Mop Holder",
                    "Under Bed Storage Bag", "Kitchen Apron and Glove Set",
                    "Anti Slip Drawer Mat Roll", "Cable Management Box",
                    "Multipurpose Wall Shelf", "Door Draft Stopper",
                    "Travel Toiletry Organizer", "Compact Folding Stool",
                ],
            }
            expansion_images = [
                "photo-1556910103-1c02745aae4d",
                "photo-1582719478250-c89cae4dc85b",
                "photo-1517433670267-08bbd4be890f",
                "photo-1571175443880-49eec7d7b7a1",
                "photo-1484154218962-a197022b5858",
            ]
            expansion_index = 0
            for category_slug, names in catalogue_expansion.items():
                for name in names:
                    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
                    cursor.execute("SELECT id FROM products WHERE slug=%s", (slug,))
                    if cursor.fetchone():
                        continue
                    price = 399 + (expansion_index % 15) * 190
                    mrp = price + 200 + (expansion_index % 4) * 90
                    image = (
                        f"https://images.unsplash.com/"
                        f"{expansion_images[expansion_index % len(expansion_images)]}"
                        "?auto=format&fit=crop&w=900&q=80"
                    )
                    cursor.execute(
                        "INSERT INTO products (category_id,name,slug,sku,brand,subcategory,"
                        "short_description,description,price,compare_at_price,image_url,"
                        "stock_quantity,low_stock_threshold,tags,is_active) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,5,%s,TRUE)",
                        (
                            category_ids[category_slug],
                            name,
                            slug,
                            f"NKO-V3-{expansion_index + 1:03d}",
                            "Nakoda Select",
                            category_slug.replace("-", " ").title(),
                            f"{name} for dependable everyday use.",
                            f"{name} with practical design and durable materials.",
                            price,
                            mrp,
                            image,
                            12 + expansion_index % 35,
                            category_slug.replace("-", ", "),
                        ),
                    )
                    expansion_index += 1
            cursor.execute(
                "INSERT INTO settings (setting_key, setting_value) VALUES ('catalog_seeded_v3', 'true') "
                "ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value)"
            )
        # UPI is intentionally seeded only for a new/empty installation. Admin
        # changes are never overwritten on subsequent application starts.
        upi_defaults = {
            "upi_enabled": "true",
            "upi_id": "monikashikharjain-1@okhdfcbank",
            "upi_display_name": "Nakoda Kitchen Wares",
        }
        for setting_key, setting_value in upi_defaults.items():
            cursor.execute(
                "SELECT setting_value FROM settings WHERE setting_key = %s",
                (setting_key,),
            )
            existing_setting = cursor.fetchone()
            if not existing_setting or not str(existing_setting.get("setting_value") or "").strip():
                cursor.execute(
                    "INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) "
                    "ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value)",
                    (setting_key, setting_value),
                )
        admin_email = (os.getenv("ADMIN_EMAIL") or "").strip().lower()
        admin_password = os.getenv("ADMIN_PASSWORD") or ""
        if admin_email and admin_password:
            cursor.execute("SELECT id FROM users WHERE email = %s", (admin_email,))
            if not cursor.fetchone():
                cursor.execute(
                    "INSERT INTO users (name, email, password_hash, role) VALUES (%s, %s, %s, 'ADMIN')",
                    (os.getenv("ADMIN_NAME", "Nakoda Admin"), admin_email, generate_password_hash(admin_password)),
                )
        connection.commit()
    finally:
        connection.close()


def generate_math_question():
    import random

    ops = {
        "+": lambda a, b: a + b,
        "-": lambda a, b: a - b,
        "*": lambda a, b: a * b,
        "/": lambda a, b: a // b,
    }
    op = random.choice(["+", "-", "*", "/"])
    a = random.randint(2, 12)
    b = random.randint(2, 9)
    if op == "/":
        b = random.randint(1, 6)
        a = b * random.randint(2, 12)
    question = f"{a} {op} {b}"
    answer = ops[op](a, b)
    return question, answer


def validate_math_question(question, submitted_answer):
    if not question or submitted_answer is None:
        return False
    question = str(question).strip()
    match = re.fullmatch(r"\s*(\d+)\s*([+\-*/])\s*(\d+)\s*", question)
    if not match:
        return False
    a, op, b = match.groups()
    a = int(a)
    b = int(b)
    try:
        if op == "+":
            expected = a + b
        elif op == "-":
            expected = a - b
        elif op == "*":
            expected = a * b
        else:
            expected = a // b
        return int(submitted_answer) == expected
    except (TypeError, ValueError):
        return False


def get_token_from_request():
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header.replace("Bearer ", "", 1).strip()
    try:
        payload = jwt.decode(token, app.config["SECRET_KEY"], algorithms=["HS256"])
    except (jwt.PyJWTError, ValueError):
        return None
    return payload


def require_auth(fn):
    from functools import wraps

    @wraps(fn)
    def wrapper(*args, **kwargs):
        payload = get_token_from_request()
        if not payload:
            return jsonify({"error": "Authentication required."}), 401
        return fn(payload, *args, **kwargs)

    return wrapper


def require_admin(fn):
    """Require a valid JWT carrying an administrative role."""
    from functools import wraps

    @wraps(fn)
    def wrapper(*args, **kwargs):
        payload = get_token_from_request()
        role = (payload or {}).get("role", "").upper()
        if not payload:
            return jsonify({"error": "Authentication required."}), 401
        if role not in {"ADMIN", "SUPER_ADMIN"}:
            return jsonify({"error": "Admin access required."}), 403
        return fn(payload, *args, **kwargs)

    return wrapper


def user_id_from_payload(payload):
    try:
        return int(payload.get("sub"))
    except (TypeError, ValueError):
        return None


def socket_conversation_allowed(conversation_id, user_id, role):
        connection = connect()
        try:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(
                "SELECT customer_id FROM support_conversations WHERE id=%s",
                (conversation_id,),
            )
            conversation = cursor.fetchone()
            if not conversation:
                return False
            return role in {"ADMIN", "SUPER_ADMIN"} or int(conversation["customer_id"]) == user_id
        finally:
            connection.close()


@socketio.on("connect")
def socket_connect(auth):
        token = (auth or {}).get("token")
        try:
            payload = jwt.decode(token or "", app.config["SECRET_KEY"], algorithms=["HS256"])
        except (jwt.PyJWTError, ValueError, TypeError):
            return False
        user_id = user_id_from_payload(payload)
        if not user_id:
            return False
        SOCKET_USERS[request.sid] = {
            "user_id": user_id,
            "role": str(payload.get("role") or "CUSTOMER").upper(),
        }
        if SOCKET_USERS[request.sid]["role"] in {"ADMIN", "SUPER_ADMIN"}:
            join_room("admin:support")


@socketio.on("disconnect")
def socket_disconnect():
        SOCKET_USERS.pop(request.sid, None)


@socketio.on("support_join")
def socket_support_join(data):
        identity = SOCKET_USERS.get(request.sid)
        try:
            conversation_id = int((data or {}).get("conversation_id") or 0)
        except (TypeError, ValueError):
            conversation_id = 0
        if not identity or not conversation_id or not socket_conversation_allowed(
            conversation_id, identity["user_id"], identity["role"]
        ):
            emit("call_error", {"message": "You cannot join this support conversation."})
            return
        join_room(f"support:{conversation_id}")


@socketio.on("call_invite")
def socket_call_invite(data):
        identity = SOCKET_USERS.get(request.sid)
        data = data or {}
        try:
            conversation_id = int(data.get("conversation_id") or 0)
        except (TypeError, ValueError):
            conversation_id = 0
        if not identity or not conversation_id or not socket_conversation_allowed(
            conversation_id, identity["user_id"], identity["role"]
        ):
            emit("call_error", {"message": "Call is not allowed for this conversation."})
            return
        emit(
            "incoming_call",
            {
                "conversation_id": conversation_id,
                "call_id": data.get("call_id"),
                "call_type": data.get("call_type", "voice"),
                "caller_name": data.get("caller_name", "Nakoda support"),
                "caller_role": identity["role"],
            },
            to=f"support:{conversation_id}",
            skip_sid=request.sid,
        )
        if identity["role"] not in {"ADMIN", "SUPER_ADMIN"}:
            emit(
                "incoming_call",
                {
                    "conversation_id": conversation_id,
                    "call_id": data.get("call_id"),
                    "call_type": data.get("call_type", "voice"),
                    "caller_name": data.get("caller_name", "Customer"),
                    "caller_role": identity["role"],
                },
                to="admin:support",
            )


@socketio.on("call_response")
def socket_call_response(data):
        identity = SOCKET_USERS.get(request.sid)
        data = data or {}
        try:
            conversation_id = int(data.get("conversation_id") or 0)
        except (TypeError, ValueError):
            conversation_id = 0
        if not identity or not conversation_id or not socket_conversation_allowed(
            conversation_id, identity["user_id"], identity["role"]
        ):
            return
        emit(
            "call_response",
            {
                "call_id": data.get("call_id"),
                "response": data.get("response"),
                "conversation_id": conversation_id,
            },
            to=f"support:{conversation_id}",
            skip_sid=request.sid,
        )
        emit(
            "call_response",
            {"call_id": data.get("call_id"), "response": data.get("response"), "conversation_id": conversation_id},
            to="admin:support",
            skip_sid=request.sid,
        )


@socketio.on("call_state")
def socket_call_state(data):
        identity = SOCKET_USERS.get(request.sid)
        data = data or {}
        try:
            conversation_id = int(data.get("conversation_id") or 0)
        except (TypeError, ValueError):
            conversation_id = 0
        if not identity or not conversation_id or not socket_conversation_allowed(
            conversation_id, identity["user_id"], identity["role"]
        ):
            return
        emit(
            "call_state",
            {"call_id": data.get("call_id"), "state": data.get("state")},
            to=f"support:{conversation_id}",
            skip_sid=request.sid,
        )
        emit(
            "call_state",
            {"call_id": data.get("call_id"), "state": data.get("state")},
            to="admin:support",
            skip_sid=request.sid,
        )


@socketio.on("call_signal")
def socket_call_signal(data):
        identity = SOCKET_USERS.get(request.sid)
        data = data or {}
        try:
            conversation_id = int(data.get("conversation_id") or 0)
        except (TypeError, ValueError):
            conversation_id = 0
        if not identity or not conversation_id or not socket_conversation_allowed(
            conversation_id, identity["user_id"], identity["role"]
        ):
            return
        emit(
            "call_signal",
            {"call_id": data.get("call_id"), "signal": data.get("signal")},
            to=f"support:{conversation_id}",
            skip_sid=request.sid,
        )
        emit(
            "call_signal",
            {"call_id": data.get("call_id"), "signal": data.get("signal"), "conversation_id": conversation_id},
            to="admin:support",
            skip_sid=request.sid,
        )


@socketio.on("call_end")
def socket_call_end(data):
        identity = SOCKET_USERS.get(request.sid)
        data = data or {}
        try:
            conversation_id = int(data.get("conversation_id") or 0)
        except (TypeError, ValueError):
            conversation_id = 0
        if identity and conversation_id and socket_conversation_allowed(
            conversation_id, identity["user_id"], identity["role"]
        ):
            emit(
                "call_ended",
                {"call_id": data.get("call_id")},
                to=f"support:{conversation_id}",
                skip_sid=request.sid,
            )
            emit(
                "call_ended",
                {"call_id": data.get("call_id")},
                to="admin:support",
                skip_sid=request.sid,
            )


def public_user(row):
    return {
        "id": row["id"],
        "username": row.get("name") or row.get("username"),
        "name": row.get("name") or row.get("username"),
        "email": row.get("email"),
        "phone": row.get("phone") or "",
        "role": row.get("role", "CUSTOMER"),
        "created_at": row.get("created_at"),
        "profile_image_url": row.get("profile_image_url"),
    }


def notify_admins(cursor, title, message, notification_type="SYSTEM", order_id=None):
    cursor.execute("SELECT id FROM users WHERE role IN ('ADMIN', 'SUPER_ADMIN')")
    for row in cursor.fetchall():
        admin_user_id = row["id"] if isinstance(row, dict) else row[0]
        cursor.execute(
            "INSERT INTO notifications (user_id, order_id, order_number, title, message, notification_type) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (admin_user_id, order_id, f"NK{int(order_id):08d}" if order_id else None,
             title, message, notification_type),
        )


def create_notification(cursor, user_id, title, message, notification_type="GENERAL",
                        order_id=None, metadata=None):
    """Persist a notification and keep order links usable by both clients."""
    cursor.execute(
        "INSERT INTO notifications "
        "(user_id, order_id, order_number, title, message, notification_type, metadata) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (user_id, order_id, f"NK{int(order_id):08d}" if order_id else None,
         title, message, notification_type,
         json.dumps(metadata, default=str) if metadata is not None else None),
    )


UPI_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{1,254}@[A-Za-z][A-Za-z0-9.-]{1,63}$")


def setting_values(cursor, keys):
    placeholders = ", ".join(["%s"] * len(keys))
    cursor.execute(
        f"SELECT setting_key, setting_value FROM settings WHERE setting_key IN ({placeholders})",
        tuple(keys),
    )
    return {row["setting_key"]: row["setting_value"] for row in cursor.fetchall()}


def public_upi_config(cursor):
    values = setting_values(cursor, ("upi_enabled", "upi_id", "upi_display_name", "upi_qr_url"))
    upi_id = str(values.get("upi_id") or "").strip()
    enabled = str(values.get("upi_enabled") or "false").lower() in {"1", "true", "yes", "on"}
    return {
        "enabled": bool(enabled and UPI_ID_PATTERN.fullmatch(upi_id or "")),
        "upi_id": upi_id,
        "display_name": str(values.get("upi_display_name") or "Nakoda Kitchen Wares").strip(),
        "qr_image_url": str(values.get("upi_qr_url") or "").strip() or None,
    }


@app.get("/uploads/<path:filename>")
def uploaded_file(filename):
    return send_from_directory(str(UPLOAD_DIR), filename)


@app.get("/")
def home():
    return send_from_directory(str(BASE_DIR), "index.html")


@app.get("/<path:page>")
def frontend_page(page):
    """Serve the static storefront and account shell for direct-link navigation."""
    if page == "admin" or page.startswith("admin/"):
        return send_from_directory(str(BASE_DIR), "admin.html")
    if page in {"login", "register", "forgot-password", "reset-password"} or page == "account" or page.startswith("account/"):
        return send_from_directory(str(BASE_DIR), "account.html")
    if page in {"index.html", "screen-1.html", "screen-2.html", "screen-3.html", "screen-4.html"}:
        return send_from_directory(str(BASE_DIR), page)
    return jsonify({"error": "Not found"}), 404


@app.get("/api/health")
def health():
    started = datetime.now(timezone.utc)
    connection = None
    try:
        connection = connect()
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        db_ms = round((datetime.now(timezone.utc) - started).total_seconds() * 1000, 2)
        free_bytes = None
        try:
            import shutil
            free_bytes = shutil.disk_usage(str(BASE_DIR)).free
        except OSError:
            pass
        return jsonify({
            "status": "ok", "database": "connected", "database_latency_ms": db_ms,
            "uploads_writable": os.access(str(UPLOAD_DIR), os.W_OK),
            "disk_free_bytes": free_bytes,
        })
    except Exception:
        return jsonify({"status": "degraded", "database": "unavailable"}), 503
    finally:
        if connection:
            connection.close()


@app.get("/api/bot-check")
def bot_check():
    question, answer = generate_math_question()
    return jsonify({"question": question, "answer": answer})


@app.get("/api/categories")
def categories():
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            "SELECT c.* FROM categories c WHERE c.is_active = 1 "
            "ORDER BY COALESCE(c.parent_id, 0), c.name"
        )
        return jsonify(cursor.fetchall())
    finally:
        connection.close()


@app.get("/api/banners")
def public_banners():
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM banners WHERE is_active = 1 AND (start_date IS NULL OR start_date <= NOW()) AND (end_date IS NULL OR end_date >= NOW()) ORDER BY priority DESC, created_at DESC"
        )
        return jsonify(cursor.fetchall())
    finally:
        connection.close()


@app.get("/api/campaigns")
def public_campaigns():
    """Active merchandising campaigns, without exposing admin-only budget fields."""
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            """SELECT id, name, slug, description, campaign_type, banner_image_url,
                      landing_url, start_date, end_date
               FROM campaigns
               WHERE status='ACTIVE' AND (start_date IS NULL OR start_date<=NOW())
                 AND (end_date IS NULL OR end_date>=NOW())
               ORDER BY COALESCE(start_date, '1970-01-01') DESC, id DESC"""
        )
        campaigns = cursor.fetchall()
        for campaign in campaigns:
            cursor.execute(
                """SELECT p.id, p.name, p.slug, p.price, p.compare_at_price, p.image_url,
                          p.stock_quantity, p.badge_label, p.is_bestseller, p.is_new_arrival
                   FROM campaign_products cp JOIN products p ON p.id=cp.product_id
                   WHERE cp.campaign_id=%s AND p.is_active=1 ORDER BY p.name""",
                (campaign["id"],),
            )
            campaign["products"] = cursor.fetchall()
        return jsonify({"campaigns": campaigns})
    finally:
        connection.close()


@app.get("/api/payment/upi-config")
def upi_config():
    """Return only the public UPI checkout details, never admin settings."""
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        return jsonify(public_upi_config(cursor))
    finally:
        connection.close()


@app.get("/api/products")
def products():
    category = (request.args.get("category") or "").strip()
    query_text = (request.args.get("q") or "").strip()
    min_price = request.args.get("min_price", type=float) or 0
    max_price = request.args.get("max_price", type=float) or 100000
    sort = request.args.get("sort", "featured")
    in_stock = request.args.get("in_stock") == "true"
    brand = (request.args.get("brand") or "").strip()
    material = (request.args.get("material") or "").strip()
    color = (request.args.get("color") or "").strip()
    subcategory = (request.args.get("subcategory") or "").strip()
    badge = (request.args.get("badge") or "").strip().lower()
    featured = request.args.get("featured") == "true"
    paginated = request.args.get("paginated") == "true"
    page = max(1, request.args.get("page", 1, type=int))
    page_size = min(60, max(1, request.args.get("size", 24, type=int)))

    sql = """
        SELECT p.*, c.name AS category_name
        FROM products p
        LEFT JOIN categories c ON c.id = p.category_id
        WHERE p.is_active = 1
    """
    params = []
    if category and category != "all":
        sql += " AND c.slug = %s"
        params.append(category)
    if query_text:
        sql += " AND (p.name LIKE %s OR p.description LIKE %s OR p.short_description LIKE %s "
        sql += "OR p.sku LIKE %s OR p.brand LIKE %s OR p.subcategory LIKE %s OR p.tags LIKE %s OR c.name LIKE %s)"
        like = f"%{query_text}%"
        params.extend([like] * 8)
    sql += " AND p.price >= %s AND p.price <= %s"
    params.extend([min_price, max_price])
    if in_stock:
        sql += " AND p.stock_quantity > 0"
    for column, value in (("brand", brand), ("material", material), ("color", color),
                          ("subcategory", subcategory)):
        if value:
            sql += f" AND p.{column} LIKE %s"
            params.append(f"%{value}%")
    if featured:
        sql += " AND p.is_featured = 1"
    if badge in {"bestseller", "new", "new_arrival", "trending", "featured"}:
        flag = {"bestseller": "is_bestseller", "new": "is_new_arrival",
                "new_arrival": "is_new_arrival", "trending": "is_trending",
                "featured": "is_featured"}[badge]
        sql += f" AND p.{flag} = 1"
    if sort == "price_low_high":
        sql += " ORDER BY p.price ASC"
    elif sort == "price_high_low":
        sql += " ORDER BY p.price DESC"
    elif sort == "popular":
        sql += " ORDER BY p.stock_quantity DESC"
    elif sort == "newest":
        sql += " ORDER BY p.created_at DESC"
    elif sort == "discount":
        sql += " ORDER BY ((p.compare_at_price - p.price) / NULLIF(p.compare_at_price, 0)) DESC, p.created_at DESC"
    else:
        sql += " ORDER BY p.is_featured DESC, p.is_bestseller DESC, p.created_at DESC"

    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        total = None
        if paginated:
            count_sql = f"SELECT COUNT(*) AS total FROM ({sql}) AS filtered_products"
            cursor.execute(count_sql, params)
            total = int(cursor.fetchone()["total"])
            sql += " LIMIT %s OFFSET %s"
            params.extend([page_size, (page - 1) * page_size])
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        if rows:
            ids = [row["id"] for row in rows]
            marks = ",".join(["%s"] * len(ids))
            cursor.execute(
                f"SELECT product_id, image_url, alt_text, sort_order, is_primary "
                f"FROM product_images WHERE product_id IN ({marks}) ORDER BY sort_order, id", ids
            )
            image_map = {}
            for image in cursor.fetchall():
                image_map.setdefault(image["product_id"], []).append(image)
            for row in rows:
                row["images"] = image_map.get(row["id"], [])
                row["badges"] = [
                    label for enabled, label in (
                        (row.get("is_bestseller"), "Bestseller"),
                        (row.get("is_new_arrival"), "New"),
                        (row.get("is_trending"), "Trending"),
                        (row.get("is_featured"), "Featured"),
                    ) if enabled
                ]
                if row.get("badge_label"):
                    row["badges"].insert(0, row["badge_label"])
        if not paginated:
            return jsonify(rows)
        return jsonify({
            "items": rows, "page": page, "size": page_size, "total": total,
            "pages": (total + page_size - 1) // page_size if total else 0,
        })
    finally:
        connection.close()


@app.get("/api/products/<int:product_id>")
def product_detail(product_id):
    """Return a product with its gallery and related catalogue metadata."""
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            "SELECT p.*, c.name AS category_name, c.slug AS category_slug "
            "FROM products p LEFT JOIN categories c ON c.id = p.category_id "
            "WHERE p.id = %s AND p.is_active = 1", (product_id,)
        )
        product = cursor.fetchone()
        if not product:
            return jsonify({"error": "Product not found."}), 404
        cursor.execute(
            "SELECT image_url, alt_text, sort_order, is_primary FROM product_images "
            "WHERE product_id=%s ORDER BY sort_order, id", (product_id,)
        )
        product["images"] = cursor.fetchall()
        product["badges"] = [
            label for enabled, label in (
                (product.get("is_bestseller"), "Bestseller"),
                (product.get("is_new_arrival"), "New"),
                (product.get("is_trending"), "Trending"),
                (product.get("is_featured"), "Featured"),
            ) if enabled
        ]
        return jsonify({"product": product})
    finally:
        connection.close()


@app.get("/api/products/slug/<slug>")
def product_detail_by_slug(slug):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT id FROM products WHERE slug=%s AND is_active=1", (slug,))
        row = cursor.fetchone()
    finally:
        connection.close()
    if not row:
        return jsonify({"error": "Product not found."}), 404
    return product_detail(row["id"])


@app.get("/api/products/<int:product_id>/recommendations")
def product_recommendations(product_id):
    """Recommendations are based on catalogue relationships and recorded purchases."""
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT category_id FROM products WHERE id=%s AND is_active=1", (product_id,))
        source = cursor.fetchone()
        if not source:
            return jsonify({"items": []})
        cursor.execute(
            """SELECT p.*, c.name AS category_name,
               COALESCE(SUM(oi.quantity), 0) AS purchased_units
               FROM products p
               LEFT JOIN categories c ON c.id=p.category_id
               LEFT JOIN order_items oi ON oi.product_id=p.id
               WHERE p.is_active=1 AND p.id<>%s AND p.category_id=%s
               GROUP BY p.id ORDER BY purchased_units DESC, p.is_featured DESC,
               p.created_at DESC LIMIT 8""",
            (product_id, source["category_id"]),
        )
        return jsonify({"items": cursor.fetchall()})
    finally:
        connection.close()


@app.get("/api/recommendations")
@require_auth
def recommendations(payload):
    """Personalised recommendations use only a customer's real order/wishlist history."""
    user_id = user_id_from_payload(payload)
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            """SELECT p.category_id, MAX(o.created_at) AS latest_order FROM order_items oi
               JOIN orders o ON o.id=oi.order_id
               JOIN products p ON p.id=oi.product_id
               WHERE o.user_id=%s GROUP BY p.category_id
               ORDER BY latest_order DESC LIMIT 5""", (user_id,)
        )
        categories_seen = [r["category_id"] for r in cursor.fetchall() if r["category_id"]]
        if categories_seen:
            marks = ",".join(["%s"] * len(categories_seen))
            cursor.execute(
                f"""SELECT p.*, c.name AS category_name,
                    COALESCE(SUM(oi.quantity),0) AS purchased_units
                    FROM products p LEFT JOIN categories c ON c.id=p.category_id
                    LEFT JOIN order_items oi ON oi.product_id=p.id
                    WHERE p.is_active=1 AND p.category_id IN ({marks})
                    AND p.id NOT IN (SELECT product_id FROM wishlists WHERE user_id=%s)
                    GROUP BY p.id ORDER BY purchased_units DESC, p.is_featured DESC,
                    p.created_at DESC LIMIT 12""", categories_seen + [user_id]
            )
        else:
            cursor.execute(
                """SELECT p.*, c.name AS category_name, COALESCE(SUM(oi.quantity),0) AS purchased_units
                   FROM products p LEFT JOIN categories c ON c.id=p.category_id
                   LEFT JOIN order_items oi ON oi.product_id=p.id
                   WHERE p.is_active=1 GROUP BY p.id
                   ORDER BY purchased_units DESC, p.is_featured DESC, p.created_at DESC LIMIT 12"""
            )
        return jsonify({"items": cursor.fetchall(), "basis": "orders" if categories_seen else "catalogue"})
    finally:
        connection.close()


@app.get("/api/me")
@require_auth
def current_user(payload):
    user_id = user_id_from_payload(payload)
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT id, name, email, phone, role, profile_image_url, created_at FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        if not user:
            return jsonify({"error": "User not found."}), 404
        return jsonify({"user": public_user(user)})
    finally:
        connection.close()


@app.post("/api/register")
def register():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or data.get("name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    phone = (data.get("phone") or "").strip()
    password = data.get("password") or ""
    confirm_password = data.get("confirmPassword") or ""
    bot_question = (data.get("botQuestion") or "").strip()
    bot_answer = data.get("botAnswer")

    if not username or not password or not confirm_password or not email:
        return jsonify({"error": "Name, email, password and confirm password are required."}), 400
    if password != confirm_password:
        return jsonify({"error": "Passwords do not match."}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters long."}), 400
    if bot_question and not validate_math_question(bot_question, bot_answer):
        return jsonify({"error": "Bot verification failed. Please solve the math question correctly."}), 400

    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT id FROM users WHERE name = %s OR email = %s", (username, email))
        if cursor.fetchone():
            return jsonify({"error": "Username already exists."}), 409

        hashed = generate_password_hash(password)
        cursor.execute(
            "INSERT INTO users (name, email, phone, password_hash, role) VALUES (%s, %s, %s, %s, 'CUSTOMER')",
            (username, email, phone, hashed),
        )
        user_id = cursor.lastrowid
        notify_admins(cursor, "New customer", f"{username} created a Nakoda account.", "NEW_CUSTOMER")
        connection.commit()
        token = jwt.encode({
            "sub": str(user_id),
            "username": username,
            "role": "CUSTOMER",
            "exp": datetime.now(timezone.utc) + timedelta(days=7),
        }, app.config["SECRET_KEY"], algorithm="HS256")
        return jsonify({"token": token, "user": public_user({"id": user_id, "name": username, "email": email, "phone": phone, "role": "CUSTOMER"})}), 201
    finally:
        connection.close()


@app.post("/api/login")
def login():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or data.get("email") or "").strip()
    password = data.get("password") or ""
    bot_question = (data.get("botQuestion") or "").strip()
    bot_answer = data.get("botAnswer")

    if not username or not password:
        return jsonify({"error": "Username and password are required."}), 400
    if bot_question and not validate_math_question(bot_question, bot_answer):
        return jsonify({"error": "Bot verification failed. Please solve the math question correctly."}), 400

    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE name = %s OR email = %s", (username, username.lower()))
        user = cursor.fetchone()
        if not user or not user.get("is_active", True) or not user.get("password_hash") or not check_password_hash(user["password_hash"], password):
            return jsonify({"error": "Invalid username or password."}), 401
        token = jwt.encode({
            "sub": str(user["id"]),
            "username": user["name"],
            "role": user.get("role", "CUSTOMER"),
            "exp": datetime.now(timezone.utc) + timedelta(days=7),
        }, app.config["SECRET_KEY"], algorithm="HS256")
        return jsonify({"token": token, "user": public_user(user)})
    finally:
        connection.close()


@app.post("/api/logout")
def logout():
    return jsonify({"success": True, "message": "Logged out successfully."})


@app.get("/api/cart")
@require_auth
def get_cart(payload):
    user_id = payload.get("sub")
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT ci.id, ci.product_id, ci.quantity, p.name, p.price, p.image_url
            FROM cart_items ci
            JOIN carts c ON c.id = ci.cart_id
            JOIN products p ON p.id = ci.product_id
            WHERE c.user_id = %s
            ORDER BY ci.id DESC
            """,
            (user_id,),
        )
        items = cursor.fetchall()
        return jsonify({"items": items})
    finally:
        connection.close()


@app.post("/api/cart/add")
@require_auth
def add_to_cart(payload):
    user_id = payload.get("sub")
    data = request.get_json(silent=True) or {}
    product_id = data.get("product_id")
    try:
        quantity = max(1, int(data.get("quantity", 1)))
    except (TypeError, ValueError):
        return jsonify({"error": "Quantity must be a positive number."}), 400
    if not product_id:
        return jsonify({"error": "Product ID is required."}), 400

    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT stock_quantity, name FROM products WHERE id=%s AND is_active=1 FOR UPDATE", (product_id,))
        product = cursor.fetchone()
        if not product:
            return jsonify({"error": "Product is unavailable."}), 404
        if int(product["stock_quantity"]) < quantity:
            return jsonify({"error": f"Only {product['stock_quantity']} of {product['name']} are available."}), 409
        cursor.execute("SELECT id FROM carts WHERE user_id = %s FOR UPDATE", (user_id,))
        cart = cursor.fetchone()
        if not cart:
            cursor.execute("INSERT INTO carts (user_id, session_key) VALUES (%s, NULL)", (user_id,))
            cart_id = cursor.lastrowid
        else:
            cart_id = cart["id"]

        cursor.execute(
            "SELECT id, quantity FROM cart_items WHERE cart_id = %s AND product_id = %s",
            (cart_id, product_id),
        )
        item = cursor.fetchone()
        if item:
            if int(item["quantity"]) + quantity > int(product["stock_quantity"]):
                return jsonify({"error": f"Only {product['stock_quantity']} of {product['name']} are available."}), 409
            cursor.execute(
                "UPDATE cart_items SET quantity = quantity + %s WHERE id = %s",
                (quantity, item["id"]),
            )
        else:
            cursor.execute(
                "INSERT INTO cart_items (cart_id, product_id, quantity) VALUES (%s, %s, %s)",
                (cart_id, product_id, quantity),
            )
        connection.commit()
        return jsonify({"success": True, "message": "Item added to cart."})
    finally:
        connection.close()


@app.post("/api/cart/merge")
@require_auth
def merge_cart(payload):
    """Merge a guest cart after login without discarding the server cart."""
    user_id = user_id_from_payload(payload)
    data = request.get_json(silent=True) or {}
    guest_items = data.get("items") or []
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM carts WHERE user_id = %s", (user_id,))
        cart = cursor.fetchone()
        cart_id = cart[0] if cart else None
        if not cart_id:
            cursor.execute("INSERT INTO carts (user_id) VALUES (%s)", (user_id,))
            cart_id = cursor.lastrowid
        for item in guest_items:
            try:
                product_id, quantity = int(item.get("product_id")), max(1, int(item.get("quantity", 1)))
            except (TypeError, ValueError):
                continue
            cursor.execute("SELECT stock_quantity FROM products WHERE id = %s AND is_active = 1 FOR UPDATE", (product_id,))
            product = cursor.fetchone()
            if not product or int(product[0]) <= 0:
                continue
            cursor.execute("SELECT id, quantity FROM cart_items WHERE cart_id = %s AND product_id = %s FOR UPDATE", (cart_id, product_id))
            existing = cursor.fetchone()
            if existing:
                new_quantity = min(int(product[0]), int(existing[1]) + quantity)
                cursor.execute("UPDATE cart_items SET quantity = %s WHERE id = %s", (new_quantity, existing[0]))
            else:
                cursor.execute(
                    "INSERT INTO cart_items (cart_id, product_id, quantity) VALUES (%s, %s, %s)",
                    (cart_id, product_id, min(int(product[0]), quantity)),
                )
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.post("/api/orders")
@require_auth
def create_order(payload):
    user_id = user_id_from_payload(payload)
    data = request.get_json(silent=True) or {}
    full_name = (data.get("full_name") or "").strip()
    phone = (data.get("phone") or "").strip()
    address = (data.get("address") or "").strip()
    payment_method = (data.get("payment_method") or "COD").strip().upper()
    if payment_method not in {"COD", "UPI"}:
        return jsonify({"error": "Choose Cash on Delivery or UPI."}), 400
    if not all([full_name, phone, address]):
        return jsonify({"error": "Full name, phone and address are required."}), 400

    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        upi = public_upi_config(cursor)
        if payment_method == "UPI":
            if not upi["enabled"]:
                return jsonify({"error": "UPI payments are currently unavailable."}), 409
            utr = str(data.get("utr") or data.get("upi_transaction_id") or "").strip()
            payment_note = str(data.get("payment_note") or "").strip()
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._/:#-]{5,99}", utr):
                return jsonify({"error": "Enter a valid UPI transaction ID / UTR (6-100 characters)."}), 400
            if len(payment_note) > 255:
                return jsonify({"error": "Payment note must be 255 characters or fewer."}), 400
            cursor.execute(
                "SELECT id FROM transactions WHERE utr = %s AND status IN "
                "('PAYMENT_VERIFICATION_PENDING', 'VERIFIED', 'COMPLETED') LIMIT 1",
                (utr,),
            )
            if cursor.fetchone():
                return jsonify({"error": "That UPI transaction ID / UTR has already been submitted."}), 409
        else:
            utr = None
            payment_note = None
        order_payment_status = "PAYMENT_VERIFICATION_PENDING" if payment_method == "UPI" else "COD_PENDING"
        cursor.execute(
            "SELECT ci.product_id, ci.quantity, p.name, p.price, p.stock_quantity, "
            "p.low_stock_threshold FROM cart_items ci JOIN carts c ON c.id = ci.cart_id "
            "JOIN products p ON p.id = ci.product_id WHERE c.user_id = %s AND p.is_active = 1 "
            "FOR UPDATE",
            (user_id,),
        )
        items = cursor.fetchall()
        if not items:
            return jsonify({"error": "Your cart is empty."}), 400
        unavailable = [
            f"{item['name']} (available: {item['stock_quantity']})"
            for item in items if int(item["stock_quantity"]) < int(item["quantity"])
        ]
        if unavailable:
            return jsonify({"error": "Insufficient stock: " + ", ".join(unavailable)}), 409

        subtotal = round(sum(item["quantity"] * item["price"] for item in items), 2)
        delivery_charge = Decimal("0") if subtotal >= 999 else Decimal("49")
        try:
            discount = Decimal(str(data.get("discount") or "0")).quantize(Decimal("0.01"))
        except (ValueError, TypeError):
            discount = Decimal("0")
        total = max(0, round(subtotal + delivery_charge - discount, 2))
        cursor.execute(
            """INSERT INTO orders
               (user_id, status, total, full_name, phone, address, payment_method,
                payment_status, subtotal, delivery_charge, discount, expected_delivery)
               VALUES (%s, 'placed', %s, %s, %s, %s, %s, %s, %s, %s, %s, DATE_ADD(CURDATE(), INTERVAL 5 DAY))""",
            (user_id, total, full_name, phone, address, payment_method, order_payment_status,
             subtotal, delivery_charge, discount),
        )
        order_id = cursor.lastrowid
        for item in items:
            cursor.execute(
                "INSERT INTO order_items (order_id, product_id, product_name, quantity, unit_price) VALUES (%s, %s, %s, %s, %s)",
                (order_id, item["product_id"], item["name"], item["quantity"], item["price"]),
            )
        cursor.execute("INSERT INTO order_status_history (order_id, status, note) VALUES (%s, 'placed', 'Order placed successfully')", (order_id,))
        transaction_id = f"TXN-{uuid.uuid4().hex[:14].upper()}"
        transaction_status = "PAYMENT_VERIFICATION_PENDING" if payment_method == "UPI" else "COD_PENDING"
        cursor.execute(
            "INSERT INTO transactions (user_id, order_id, transaction_id, utr, payment_note, amount, "
            "payment_method, transaction_type, status) VALUES (%s, %s, %s, %s, %s, %s, %s, "
            "'ORDER_PAYMENT', %s)",
            (user_id, order_id, transaction_id, utr, payment_note, total, payment_method,
             transaction_status),
        )
        for item in items:
            cursor.execute(
                "UPDATE products SET stock_quantity = stock_quantity - %s "
                "WHERE id = %s AND stock_quantity >= %s",
                (item["quantity"], item["product_id"], item["quantity"]),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                return jsonify({"error": f"Stock changed for {item['name']}; please review your cart."}), 409
            new_stock = int(item["stock_quantity"]) - int(item["quantity"])
            cursor.execute(
                "INSERT INTO inventory_movements "
                "(product_id, previous_quantity, new_quantity, difference, reason) "
                "VALUES (%s,%s,%s,%s,%s)",
                (item["product_id"], item["stock_quantity"], new_stock, -int(item["quantity"]),
                 f"Order NK{order_id:08d}"),
            )
            if new_stock == 0:
                notify_admins(cursor, "Out of stock", f"{item['name']} is now out of stock.", "OUT_OF_STOCK")
            elif int(item["stock_quantity"]) > int(item["low_stock_threshold"]) >= new_stock:
                notify_admins(cursor, "Low stock", f"{item['name']} has only {new_stock} left.", "LOW_STOCK")
        create_notification(
            cursor, user_id, "Order placed",
            f"Your order #NK{order_id:08d} has been placed successfully.",
            "ORDER_PLACED", order_id,
        )
        notify_admins(
            cursor, "New order received",
            f"Order NK{order_id:08d} · Customer: {full_name} · Amount: ₹{total} · Payment: {payment_method}",
            "NEW_ORDER", order_id,
        )
        if payment_method == "UPI":
            notify_admins(
                cursor, "Payment verification required",
                f"UPI payment for order NK{order_id:08d} needs verification · UTR: {utr}",
                "PAYMENT_VERIFICATION_REQUIRED", order_id,
            )
            create_notification(
                cursor, user_id, "UPI payment submitted",
                f"Your payment reference for order NK{order_id:08d} is pending verification.",
                "PAYMENT_VERIFICATION_PENDING", order_id,
            )
        cursor.execute("DELETE FROM cart_items WHERE cart_id IN (SELECT id FROM carts WHERE user_id = %s)", (user_id,))
        connection.commit()
        return jsonify({
            "success": True, "order_id": order_id, "order_number": f"NK{order_id:08d}",
            "total": total, "payment_status": order_payment_status,
        })
    finally:
        connection.close()


@app.post("/api/account/orders/<order_number>/upi-payment")
@app.post("/api/orders/<order_number>/upi-payment")
@require_auth
def submit_upi_payment(payload, order_number):
    user_id = user_id_from_payload(payload)
    data = request.get_json(silent=True) or {}
    utr = str(data.get("utr") or data.get("upi_transaction_id") or "").strip()
    payment_note = str(data.get("payment_note") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 ._/:#-]{5,99}", utr):
        return jsonify({"error": "Enter a valid UPI transaction ID / UTR (6-100 characters)."}), 400
    if len(payment_note) > 255:
        return jsonify({"error": "Payment note must be 255 characters or fewer."}), 400
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        order = get_order_for_user(cursor, user_id, order_number)
        if not order:
            return jsonify({"error": "Order not found."}), 404
        if order["payment_method"] != "UPI":
            return jsonify({"error": "This order does not use UPI."}), 409
        if order["payment_status"] in {"VERIFIED", "COMPLETED"}:
            return jsonify({"error": "This payment has already been verified."}), 409
        cursor.execute(
            "SELECT id FROM transactions WHERE order_id = %s AND payment_method = 'UPI' "
            "ORDER BY created_at DESC LIMIT 1",
            (order["id"],),
        )
        transaction = cursor.fetchone()
        if not transaction:
            return jsonify({"error": "UPI transaction record not found."}), 404
        cursor.execute(
            "SELECT id FROM transactions WHERE utr = %s AND id <> %s AND status IN "
            "('PAYMENT_VERIFICATION_PENDING', 'VERIFIED', 'COMPLETED') LIMIT 1",
            (utr, transaction["id"]),
        )
        if cursor.fetchone():
            return jsonify({"error": "That UPI transaction ID / UTR has already been submitted."}), 409
        cursor.execute(
            "UPDATE transactions SET utr = %s, payment_note = %s, status = 'PAYMENT_VERIFICATION_PENDING', "
            "rejection_reason = NULL, verified_by = NULL, verified_at = NULL WHERE id = %s",
            (utr, payment_note or None, transaction["id"]),
        )
        cursor.execute(
            "UPDATE orders SET payment_status = 'PAYMENT_VERIFICATION_PENDING' WHERE id = %s",
            (order["id"],),
        )
        notify_admins(
            cursor, "Payment verification required",
            f"UPI payment for order {order['order_number']} needs verification · UTR: {utr}",
            "PAYMENT_VERIFICATION_REQUIRED", order["id"],
        )
        create_notification(
            cursor, user_id, "UPI payment submitted",
            f"Your payment reference for order {order['order_number']} is pending verification.",
            "PAYMENT_VERIFICATION_PENDING", order["id"],
        )
        connection.commit()
        return jsonify({"success": True, "payment_status": "PAYMENT_VERIFICATION_PENDING"})
    finally:
        connection.close()


def order_id_from_number(order_number):
    value = str(order_number or "").strip().upper()
    if value.startswith("NK"):
        value = value[2:]
    try:
        return int(value)
    except ValueError:
        return None


def order_query():
    return """SELECT o.*, CONCAT('NK', LPAD(o.id, 8, '0')) AS order_number
              FROM orders o WHERE o.user_id = %s"""


@app.get("/api/account")
@require_auth
def account_dashboard(payload):
    user_id = user_id_from_payload(payload)
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT id, name, email, phone, role, profile_image_url, created_at FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        cursor.execute("SELECT status, COUNT(*) AS total FROM orders WHERE user_id = %s GROUP BY status", (user_id,))
        counts = {row["status"]: int(row["total"]) for row in cursor.fetchall()}
        cursor.execute(order_query() + " ORDER BY o.created_at DESC LIMIT 5", (user_id,))
        orders = cursor.fetchall()
        for order in orders:
            cursor.execute("SELECT product_name, quantity, unit_price FROM order_items WHERE order_id = %s", (order["id"],))
            order["items"] = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) AS total FROM notifications WHERE user_id = %s AND is_read = 0", (user_id,))
        unread = cursor.fetchone()["total"]
        return jsonify({"user": public_user(user), "summary": {
            "total_orders": sum(counts.values()),
            "active_orders": sum(v for k, v in counts.items() if k not in ("delivered", "cancelled", "returned")),
            "delivered_orders": counts.get("delivered", 0),
            "cancelled_orders": counts.get("cancelled", 0),
        }, "orders": orders, "unread_notifications": unread})
    finally:
        connection.close()


@app.get("/api/account/orders")
@require_auth
def account_orders(payload):
    user_id = user_id_from_payload(payload)
    status = (request.args.get("status") or "").strip().lower()
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        sql = order_query()
        params = [user_id]
        if status and status != "all":
            statuses = {"active": ("placed", "confirmed", "processing", "packed", "out_for_delivery"),
                        "delivered": ("delivered",), "cancelled": ("cancelled",), "returned": ("returned",)}
            selected = statuses.get(status, (status,))
            sql += " AND o.status IN (" + ",".join(["%s"] * len(selected)) + ")"
            params.extend(selected)
        sql += " ORDER BY o.created_at DESC"
        cursor.execute(sql, params)
        orders = cursor.fetchall()
        for order in orders:
            cursor.execute("SELECT oi.*, p.image_url FROM order_items oi LEFT JOIN products p ON p.id = oi.product_id WHERE oi.order_id = %s", (order["id"],))
            order["items"] = cursor.fetchall()
            cursor.execute("SELECT transaction_id, utr, payment_note, amount, payment_method, status, verified_at "
                           "FROM transactions WHERE order_id = %s ORDER BY created_at DESC", (order["id"],))
            order["transactions"] = cursor.fetchall()
        return jsonify({"orders": orders})
    finally:
        connection.close()


@app.get("/api/orders")
@require_auth
def orders_alias(payload):
    return account_orders.__wrapped__(payload)


def get_order_for_user(cursor, user_id, order_number):
    order_id = order_id_from_number(order_number)
    if not order_id:
        return None
    cursor.execute(order_query() + " AND o.id = %s", (user_id, order_id))
    return cursor.fetchone()


@app.get("/api/account/orders/<order_number>")
@require_auth
def account_order_detail(payload, order_number):
    user_id = user_id_from_payload(payload)
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        order = get_order_for_user(cursor, user_id, order_number)
        if not order:
            return jsonify({"error": "Order not found."}), 404
        cursor.execute("SELECT oi.*, p.image_url FROM order_items oi LEFT JOIN products p ON p.id = oi.product_id WHERE oi.order_id = %s", (order["id"],))
        order["items"] = cursor.fetchall()
        cursor.execute("SELECT transaction_id, utr, payment_note, amount, payment_method, status, verified_at "
                       "FROM transactions WHERE order_id = %s ORDER BY created_at DESC", (order["id"],))
        order["transactions"] = cursor.fetchall()
        cursor.execute("SELECT status, note, created_at FROM order_status_history WHERE order_id = %s ORDER BY created_at ASC", (order["id"],))
        order["timeline"] = cursor.fetchall()
        if not order["timeline"]:
            order["timeline"] = [{"status": order["status"], "created_at": order["created_at"]}]
        return jsonify({"order": order})
    finally:
        connection.close()


@app.get("/api/orders/<order_number>")
@require_auth
def order_detail_alias(payload, order_number):
    return account_order_detail.__wrapped__(payload, order_number)


@app.get("/api/account/orders/<order_number>/tracking")
@require_auth
def account_order_tracking(payload, order_number):
    user_id = user_id_from_payload(payload)
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        order = get_order_for_user(cursor, user_id, order_number)
        if not order:
            return jsonify({"error": "Order not found."}), 404
        cursor.execute("SELECT status, note, created_at FROM order_status_history WHERE order_id = %s ORDER BY created_at ASC", (order["id"],))
        return jsonify({"order_number": order["order_number"], "current_status": order["status"], "timeline": cursor.fetchall()})
    finally:
        connection.close()


@app.post("/api/account/orders/<order_number>/cancel")
@require_auth
def cancel_order(payload, order_number):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        order = get_order_for_user(cursor, user_id_from_payload(payload), order_number)
        if not order or order["status"] in ("delivered", "cancelled", "returned", "out_for_delivery"):
            return jsonify({"error": "This order can no longer be cancelled."}), 409
        cursor.execute(
            "SELECT product_id, quantity FROM order_items WHERE order_id=%s AND product_id IS NOT NULL",
            (order["id"],),
        )
        for item in cursor.fetchall():
            cursor.execute(
                "SELECT stock_quantity FROM products WHERE id=%s FOR UPDATE", (item["product_id"],)
            )
            stock = cursor.fetchone()
            if stock:
                previous = int(stock["stock_quantity"])
                new_stock = previous + int(item["quantity"])
                cursor.execute("UPDATE products SET stock_quantity=%s WHERE id=%s",
                               (new_stock, item["product_id"]))
                cursor.execute(
                    "INSERT INTO inventory_movements "
                    "(product_id,previous_quantity,new_quantity,difference,reason) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (item["product_id"], previous, new_stock, int(item["quantity"]),
                     f"Order {order['order_number']} cancelled"),
                )
        cursor.execute("UPDATE orders SET status = 'cancelled' WHERE id = %s", (order["id"],))
        cursor.execute("INSERT INTO order_status_history (order_id, status, note) VALUES (%s, 'cancelled', 'Cancelled by customer')", (order["id"],))
        create_notification(cursor, order["user_id"], "Order cancelled",
                            f"Your order #{order['order_number']} was cancelled.",
                            "ORDER_CANCELLED", order["id"])
        notify_admins(cursor, "Order cancelled",
                      f"Customer cancelled order {order['order_number']}.",
                      "ORDER_CANCELLED", order["id"])
        connection.commit()
        return jsonify({"success": True, "status": "cancelled"})
    finally:
        connection.close()


@app.post("/api/account/orders/<order_number>/return")
@require_auth
def return_order(payload, order_number):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        order = get_order_for_user(cursor, user_id_from_payload(payload), order_number)
        if not order or order["status"] != "delivered":
            return jsonify({"error": "Returns are available after delivery."}), 409
        cursor.execute("UPDATE orders SET status = 'returned' WHERE id = %s", (order["id"],))
        cursor.execute("INSERT INTO order_status_history (order_id, status, note) VALUES (%s, 'returned', 'Return requested by customer')", (order["id"],))
        create_notification(cursor, order["user_id"], "Return requested",
                            f"Your return request for order #{order['order_number']} was received.",
                            "RETURN_REQUESTED", order["id"])
        notify_admins(cursor, "Return request",
                      f"Return requested for order {order['order_number']}.",
                      "RETURN_REQUEST", order["id"])
        connection.commit()
        return jsonify({"success": True, "status": "returned"})
    finally:
        connection.close()


@app.post("/api/account/orders/<order_number>/buy-again")
@require_auth
def buy_again(payload, order_number):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        order = get_order_for_user(cursor, user_id_from_payload(payload), order_number)
        if not order:
            return jsonify({"error": "Order not found."}), 404
        cursor.execute("SELECT product_id, quantity FROM order_items WHERE order_id = %s AND product_id IS NOT NULL", (order["id"],))
        items = cursor.fetchall()
        cursor.execute("SELECT id FROM carts WHERE user_id = %s", (order["user_id"],))
        cart = cursor.fetchone()
        cart_id = cart["id"] if cart else None
        if not cart_id:
            cursor.execute("INSERT INTO carts (user_id) VALUES (%s)", (order["user_id"],))
            cart_id = cursor.lastrowid
        for item in items:
            cursor.execute(
                "SELECT stock_quantity FROM products WHERE id=%s AND is_active=1 FOR UPDATE",
                (item["product_id"],),
            )
            product = cursor.fetchone()
            if not product or int(product[0]) <= 0:
                continue
            cursor.execute("SELECT id, quantity FROM cart_items WHERE cart_id=%s AND product_id=%s FOR UPDATE",
                           (cart_id, item["product_id"]))
            existing = cursor.fetchone()
            if existing:
                quantity = min(int(product[0]), int(existing[1]) + int(item["quantity"]))
                cursor.execute("UPDATE cart_items SET quantity=%s WHERE id=%s", (quantity, existing[0]))
            else:
                cursor.execute(
                    "INSERT INTO cart_items (cart_id, product_id, quantity) VALUES (%s, %s, %s)",
                    (cart_id, item["product_id"], min(int(product[0]), int(item["quantity"]))),
                )
        connection.commit()
        return jsonify({"success": True, "items_added": len(items)})
    finally:
        connection.close()


@app.post("/api/account/orders/<order_number>/support")
@require_auth
def order_support(payload, order_number):
    data = request.get_json(silent=True) or {}
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        order = get_order_for_user(cursor, user_id_from_payload(payload), order_number)
        if not order:
            return jsonify({"error": "Order not found."}), 404
        cursor.execute("SELECT name, email, phone FROM users WHERE id = %s", (order["user_id"],))
        user = cursor.fetchone()
        message = (data.get("message") or f"Help needed with order {order['order_number']}").strip()
        cursor.execute("INSERT INTO enquiries (name, email, phone, message) VALUES (%s, %s, %s, %s)", (user["name"], user["email"], user["phone"] or "not provided", message))
        connection.commit()
        return jsonify({"success": True, "message": "Nakoda support has received your request."}), 201
    finally:
        connection.close()


@app.put("/api/account/profile")
@require_auth
def update_profile(payload):
    user_id = user_id_from_payload(payload)
    data = request.get_json(silent=True) or {}
    name, email, phone = (str(data.get(key) or "").strip() for key in ("name", "email", "phone"))
    if not name or not email:
        return jsonify({"error": "Name and email are required."}), 400
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT id FROM users WHERE email = %s AND id <> %s", (email.lower(), user_id))
        if cursor.fetchone():
            return jsonify({"error": "That email is already in use."}), 409
        cursor.execute("UPDATE users SET name = %s, email = %s, phone = %s WHERE id = %s", (name, email.lower(), phone, user_id))
        connection.commit()
        cursor.execute("SELECT id, name, email, phone, role, profile_image_url, created_at FROM users WHERE id = %s", (user_id,))
        return jsonify({"user": public_user(cursor.fetchone())})
    finally:
        connection.close()


ADDRESS_FIELDS = ("full_name", "mobile", "house_flat", "street", "area", "landmark", "city", "state", "pincode", "address_type")


@app.get("/api/account/addresses")
@require_auth
def list_addresses(payload):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM addresses WHERE user_id = %s ORDER BY is_default DESC, updated_at DESC", (user_id_from_payload(payload),))
        return jsonify({"addresses": cursor.fetchall()})
    finally:
        connection.close()


@app.post("/api/account/addresses")
@require_auth
def create_address(payload):
    data = request.get_json(silent=True) or {}
    required = ("full_name", "mobile", "house_flat", "street", "city", "state", "pincode")
    if any(not str(data.get(key) or "").strip() for key in required):
        return jsonify({"error": "Complete name, mobile and delivery address are required."}), 400
    connection = connect()
    try:
        cursor = connection.cursor()
        user_id = user_id_from_payload(payload)
        is_default = bool(data.get("is_default"))
        if is_default:
            cursor.execute("UPDATE addresses SET is_default = 0 WHERE user_id = %s", (user_id,))
        cursor.execute(
            "INSERT INTO addresses (user_id, full_name, mobile, house_flat, street, area, landmark, city, state, pincode, address_type, is_default) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (user_id,) + tuple(str(data.get(k) or "").strip() for k in ADDRESS_FIELDS) + (is_default,),
        )
        connection.commit()
        return jsonify({"id": cursor.lastrowid}), 201
    finally:
        connection.close()


@app.put("/api/account/addresses/<int:address_id>")
@require_auth
def update_address(payload, address_id):
    data = request.get_json(silent=True) or {}
    connection = connect()
    try:
        cursor = connection.cursor()
        user_id = user_id_from_payload(payload)
        if data.get("is_default"):
            cursor.execute("UPDATE addresses SET is_default = 0 WHERE user_id = %s", (user_id,))
        fields = [k for k in ADDRESS_FIELDS if k in data]
        if fields:
            sql = "UPDATE addresses SET " + ", ".join(f"{field} = %s" for field in fields) + ", is_default = %s WHERE id = %s AND user_id = %s"
            cursor.execute(sql, tuple(data[field] for field in fields) + (bool(data.get("is_default")), address_id, user_id))
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.delete("/api/account/addresses/<int:address_id>")
@require_auth
def delete_address(payload, address_id):
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM addresses WHERE id = %s AND user_id = %s", (address_id, user_id_from_payload(payload)))
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.post("/api/account/addresses/<int:address_id>/default")
@require_auth
def default_address(payload, address_id):
    connection = connect()
    try:
        cursor = connection.cursor()
        user_id = user_id_from_payload(payload)
        cursor.execute("UPDATE addresses SET is_default = 0 WHERE user_id = %s", (user_id,))
        cursor.execute("UPDATE addresses SET is_default = 1 WHERE id = %s AND user_id = %s", (address_id, user_id))
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.get("/api/account/wishlist")
@require_auth
def list_wishlist(payload):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("""SELECT w.id, w.product_id, w.created_at, p.name, p.price,
                          p.compare_at_price, p.image_url, p.stock_quantity
                          FROM wishlists w JOIN products p ON p.id = w.product_id
                          WHERE w.user_id = %s ORDER BY w.created_at DESC""", (user_id_from_payload(payload),))
        return jsonify({"items": cursor.fetchall()})
    finally:
        connection.close()


@app.post("/api/account/wishlist")
@require_auth
def add_wishlist(payload):
    product_id = (request.get_json(silent=True) or {}).get("product_id")
    if not product_id:
        return jsonify({"error": "Product ID is required."}), 400
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("INSERT IGNORE INTO wishlists (user_id, product_id) SELECT %s, id FROM products WHERE id = %s AND is_active = 1", (user_id_from_payload(payload), product_id))
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.delete("/api/account/wishlist/<int:product_id>")
@require_auth
def remove_wishlist(payload, product_id):
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM wishlists WHERE user_id = %s AND product_id = %s", (user_id_from_payload(payload), product_id))
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.post("/api/account/recently-viewed")
@require_auth
def record_recently_viewed(payload):
    product_id = (request.get_json(silent=True) or {}).get("product_id")
    if not product_id:
        return jsonify({"error": "Product ID is required."}), 400
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("""INSERT INTO recently_viewed (user_id, product_id) VALUES (%s, %s)
                          ON DUPLICATE KEY UPDATE viewed_at = CURRENT_TIMESTAMP""", (user_id_from_payload(payload), product_id))
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.get("/api/account/recently-viewed")
@require_auth
def recently_viewed(payload):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("""SELECT p.*, rv.viewed_at FROM recently_viewed rv
                          JOIN products p ON p.id = rv.product_id WHERE rv.user_id = %s
                          ORDER BY rv.viewed_at DESC LIMIT 30""", (user_id_from_payload(payload),))
        return jsonify({"items": cursor.fetchall()})
    finally:
        connection.close()


@app.get("/api/account/transactions")
@require_auth
def account_transactions(payload):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("""SELECT t.*, CONCAT('NK', LPAD(t.order_id, 8, '0')) AS order_number
                          FROM transactions t WHERE t.user_id = %s ORDER BY t.created_at DESC""", (user_id_from_payload(payload),))
        return jsonify({"transactions": cursor.fetchall()})
    finally:
        connection.close()


@app.get("/api/account/notifications")
@require_auth
def account_notifications(payload):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            "SELECT n.*, COALESCE(n.order_number, CONCAT('NK', LPAD(n.order_id, 8, '0'))) AS order_number "
            "FROM notifications n WHERE n.user_id = %s ORDER BY n.created_at DESC LIMIT 100",
            (user_id_from_payload(payload),),
        )
        rows = cursor.fetchall()
        return jsonify({"notifications": rows, "unread_count": sum(not row["is_read"] for row in rows)})
    finally:
        connection.close()


@app.post("/api/account/notifications/<int:notification_id>/read")
@require_auth
def mark_notification_read(payload, notification_id):
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("UPDATE notifications SET is_read = 1 WHERE id = %s AND user_id = %s", (notification_id, user_id_from_payload(payload)))
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.post("/api/account/notifications/read-all")
@require_auth
def mark_all_notifications_read(payload):
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("UPDATE notifications SET is_read = 1 WHERE user_id = %s", (user_id_from_payload(payload),))
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.post("/api/forgot-password")
def forgot_password():
    email = ((request.get_json(silent=True) or {}).get("email") or "").strip().lower()
    if not email:
        return jsonify({"error": "Email is required."}), 400
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
        user = cursor.fetchone()
        response = {"success": True, "message": "If that email exists, reset instructions have been sent."}
        if user:
            raw_token = secrets.token_urlsafe(32)
            token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
            cursor.execute("INSERT INTO password_reset_tokens (user_id, token_hash, expires_at) VALUES (%s, %s, %s)", (user["id"], token_hash, datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=1)))
            connection.commit()
            # A mail provider can consume this token; returning it also keeps local deployments testable.
            response["reset_token"] = raw_token
        return jsonify(response)
    finally:
        connection.close()


@app.post("/api/reset-password")
def reset_password():
    data = request.get_json(silent=True) or {}
    raw_token, password = data.get("token") or "", data.get("password") or ""
    if len(password) < 6 or not raw_token:
        return jsonify({"error": "A valid token and a password of at least 6 characters are required."}), 400
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        digest = hashlib.sha256(raw_token.encode()).hexdigest()
        cursor.execute("SELECT id, user_id FROM password_reset_tokens WHERE token_hash = %s AND used_at IS NULL AND expires_at > UTC_TIMESTAMP()", (digest,))
        token = cursor.fetchone()
        if not token:
            return jsonify({"error": "Reset token is invalid or expired."}), 400
        cursor.execute("UPDATE users SET password_hash = %s WHERE id = %s", (generate_password_hash(password), token["user_id"]))
        cursor.execute("UPDATE password_reset_tokens SET used_at = UTC_TIMESTAMP() WHERE id = %s", (token["id"],))
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.patch("/api/admin/orders/<order_number>/status")
@require_admin
def admin_update_order_status(payload, order_number):
    status = ((request.get_json(silent=True) or {}).get("status") or "").strip().lower()
    allowed = {"placed", "confirmed", "processing", "packed", "out_for_delivery", "delivered", "cancelled", "return_requested", "returned", "refund_initiated", "refunded"}
    if status not in allowed:
        return jsonify({"error": "Unsupported order status."}), 400
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        order_id = order_id_from_number(order_number)
        cursor.execute("SELECT id, user_id, status, payment_method, payment_status FROM orders WHERE id = %s", (order_id,))
        order = cursor.fetchone()
        if not order:
            return jsonify({"error": "Order not found."}), 404
        if (
            status in {"confirmed", "processing", "packed", "out_for_delivery", "delivered"}
            and order["payment_method"] == "UPI"
            and order["payment_status"] != "VERIFIED"
        ):
            return jsonify({"error": "Verify the UPI payment before advancing this order."}), 409
        transitions = {
            "pending": {"confirmed", "cancelled"},
            "placed": {"confirmed", "cancelled"},
            "confirmed": {"processing", "cancelled"},
            "processing": {"packed", "cancelled"},
            "packed": {"out_for_delivery", "cancelled"},
            "out_for_delivery": {"delivered"},
            "delivered": {"return_requested", "returned"},
            "cancelled": set(),
            "return_requested": {"returned", "cancelled"},
            "returned": {"refund_initiated"},
            "refund_initiated": {"refunded"},
            "refunded": set(),
        }
        if status != order["status"] and status not in transitions.get(order["status"], set()):
            return jsonify({"error": f"Cannot move an order from {order['status']} to {status}."}), 409
        if status == order["status"]:
            return jsonify({"success": True, "status": status, "changed": False})
        if status == "cancelled" and order["status"] not in {"delivered", "returned", "refunded"}:
            cursor.execute(
                "SELECT product_id, quantity FROM order_items "
                "WHERE order_id=%s AND product_id IS NOT NULL", (order_id,)
            )
            for item in cursor.fetchall():
                cursor.execute(
                    "SELECT stock_quantity FROM products WHERE id=%s FOR UPDATE",
                    (item["product_id"],),
                )
                stock = cursor.fetchone()
                if stock:
                    previous = int(stock["stock_quantity"])
                    restored = previous + int(item["quantity"])
                    cursor.execute("UPDATE products SET stock_quantity=%s WHERE id=%s",
                                   (restored, item["product_id"]))
                    cursor.execute(
                        "INSERT INTO inventory_movements "
                        "(product_id,previous_quantity,new_quantity,difference,reason,admin_id) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (item["product_id"], previous, restored, int(item["quantity"]),
                         f"Order {order_id} cancelled by admin", admin_id(payload)),
                    )
        cursor.execute("UPDATE orders SET status = %s, payment_status = CASE WHEN %s = 'delivered' AND payment_method = 'COD' THEN 'COMPLETED' WHEN %s = 'refunded' THEN 'REFUNDED' ELSE payment_status END WHERE id = %s", (status, status, status, order_id))
        if status == "delivered" and order["status"] != "delivered":
            cursor.execute(
                "UPDATE transactions SET status = 'COMPLETED', verified_at = UTC_TIMESTAMP() "
                "WHERE order_id = %s AND payment_method = 'COD' AND status = 'COD_PENDING'",
                (order_id,),
            )
        cursor.execute("INSERT INTO order_status_history (order_id, status) VALUES (%s, %s)", (order_id, status))
        status_copy = status.replace("_", " ")
        customer_messages = {
            "confirmed": ("Order confirmed", "Your order has been confirmed by Nakoda."),
            "processing": ("Order processing", "Your order is being processed."),
            "packed": ("Order packed", "Your order has been packed and is ready for delivery."),
            "out_for_delivery": ("Out for delivery", "Your order is out for delivery."),
            "delivered": ("Order delivered", "Your order has been delivered successfully."),
            "cancelled": ("Order cancelled", "Your order has been cancelled."),
            "return_requested": ("Return requested", "Your return request has been received."),
            "returned": ("Return approved", "Your return has been approved."),
            "refund_initiated": ("Refund initiated", "Your refund has been initiated."),
            "refunded": ("Refund completed", "Your refund has been completed."),
        }
        if order["user_id"]:
            title, message = customer_messages.get(
                status, (f"Order {status_copy.title()}", f"Your order is now {status_copy}.")
            )
            create_notification(cursor, order["user_id"], title, message,
                                "ORDER_" + status.upper(), order_id)
        write_audit(cursor, payload, "Order status changed", "order", order_id, order["status"], status)
        connection.commit()
        return jsonify({"success": True, "status": status})
    finally:
        connection.close()


def admin_id(payload):
    return user_id_from_payload(payload)


def write_audit(cursor, payload, action, entity, entity_id="", old_value=None, new_value=None):
    import json

    cursor.execute(
        "INSERT INTO audit_logs (admin_id, action, entity, entity_id, old_value, new_value) VALUES (%s, %s, %s, %s, %s, %s)",
        (admin_id(payload), action, entity, str(entity_id), json.dumps(old_value, default=str) if old_value is not None else None,
         json.dumps(new_value, default=str) if new_value is not None else None),
    )


def product_payload(data, existing=None):
    existing = existing or {}
    fields = ("name", "sku", "description", "short_description", "brand", "subcategory", "price",
              "compare_at_price", "image_url", "stock_quantity", "low_stock_threshold", "weight",
              "dimensions", "material", "color", "size", "capacity", "tags", "category_id",
              "badge_label", "badge_color")
    values = {}
    for field in fields:
        if field in data:
            values[field] = data[field]
    values["is_active"] = bool(data.get("is_active", existing.get("is_active", True)))
    for flag in ("is_featured", "is_bestseller", "is_new_arrival", "is_trending"):
        values[flag] = bool(data.get(flag, existing.get(flag, False)))
    if "name" in values and not str(values["name"]).strip():
        raise ValueError("Product name is required.")
    if "price" in values:
        try:
            values["price"] = Decimal(str(values["price"]))
        except (TypeError, ValueError):
            raise ValueError("Price must be a number.")
        if values["price"] <= 0:
            raise ValueError("Price must be greater than zero.")
    for money_field in ("compare_at_price",):
        if money_field in values:
            if values[money_field] in ("", None):
                values[money_field] = None
            else:
                try:
                    values[money_field] = Decimal(str(values[money_field]))
                except (TypeError, ValueError):
                    raise ValueError("Compare price must be a number.")
                if values["compare_at_price"] <= 0:
                    raise ValueError("MRP must be greater than zero.")
        effective_price = values.get("price", existing.get("price"))
        effective_mrp = values.get("compare_at_price", existing.get("compare_at_price"))
        if effective_price is not None and effective_mrp is not None and Decimal(str(effective_price)) > Decimal(str(effective_mrp)):
            raise ValueError("Selling price cannot exceed MRP.")
    if "category_id" in values and values["category_id"] in ("", None):
        values["category_id"] = None
    if "stock_quantity" in values:
        try:
            values["stock_quantity"] = int(values["stock_quantity"])
        except (TypeError, ValueError):
            raise ValueError("Stock must be a whole number.")
        if values["stock_quantity"] < 0:
            raise ValueError("Stock cannot be negative.")
    if "low_stock_threshold" in values:
        try:
            values["low_stock_threshold"] = int(values["low_stock_threshold"])
        except (TypeError, ValueError):
            raise ValueError("Low stock threshold must be a whole number.")
        if values["low_stock_threshold"] < 0:
            raise ValueError("Low stock threshold cannot be negative.")
    if "sku" in values and values["sku"] not in ("", None):
        values["sku"] = str(values["sku"]).strip().upper()
        if not re.fullmatch(r"[A-Z0-9][A-Z0-9._-]{1,79}", values["sku"]):
            raise ValueError("SKU must be 2-80 characters using letters, numbers, ., _ or -.")
    return values


@app.post("/api/admin/login")
def admin_login():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or data.get("email") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"error": "Username and password are required."}), 400
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE name = %s OR email = %s", (username, username.lower()))
        user = cursor.fetchone()
        if not user or not user.get("is_active", True) or (user.get("role") or "CUSTOMER").upper() not in {"ADMIN", "SUPER_ADMIN"} or not check_password_hash(user.get("password_hash", ""), password):
            return jsonify({"error": "Invalid admin credentials."}), 401
        cursor.execute("UPDATE users SET last_login = UTC_TIMESTAMP() WHERE id = %s", (user["id"],))
        connection.commit()
        days = 30 if data.get("remember") else 1
        token = jwt.encode({"sub": str(user["id"]), "username": user["name"], "role": user["role"].upper(),
                            "exp": datetime.now(timezone.utc) + timedelta(days=days)}, app.config["SECRET_KEY"], algorithm="HS256")
        return jsonify({"token": token, "user": public_user(user)})
    finally:
        connection.close()


@app.get("/api/admin/dashboard")
@require_admin
def admin_dashboard(payload):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT COALESCE(SUM(total),0) AS total_revenue, COALESCE(SUM(CASE WHEN DATE(created_at)=CURDATE() THEN total ELSE 0 END),0) AS today_revenue, COUNT(*) AS total_orders, SUM(status IN ('placed','confirmed','processing','packed')) AS pending_orders, SUM(status='delivered') AS delivered_orders FROM orders")
        kpi = cursor.fetchone()
        cursor.execute("SELECT COUNT(*) AS customers FROM users WHERE role = 'CUSTOMER'")
        kpi["customers"] = cursor.fetchone()["customers"]
        cursor.execute("SELECT COUNT(*) AS products, SUM(stock_quantity <= low_stock_threshold AND stock_quantity > 0) AS low_stock, SUM(stock_quantity = 0) AS out_of_stock FROM products WHERE is_active = 1")
        kpi.update(cursor.fetchone())
        cursor.execute("SELECT status, COUNT(*) AS total FROM orders GROUP BY status")
        statuses = cursor.fetchall()
        cursor.execute("SELECT o.*, CONCAT('NK', LPAD(o.id, 8, '0')) AS order_number, u.name AS customer_name, u.email AS customer_email FROM orders o LEFT JOIN users u ON u.id=o.user_id ORDER BY o.created_at DESC LIMIT 8")
        recent_orders = cursor.fetchall()
        cursor.execute("SELECT id, action, entity, entity_id, created_at FROM audit_logs ORDER BY created_at DESC LIMIT 10")
        activity = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) AS unread FROM notifications WHERE user_id = %s AND is_read = 0", (admin_id(payload),))
        return jsonify({"kpis": kpi, "statuses": statuses, "recent_orders": recent_orders, "activity": activity, "unread_notifications": cursor.fetchone()["unread"]})
    finally:
        connection.close()


@app.get("/api/admin/orders")
@require_admin
def admin_orders(payload):
    search = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    page = max(1, request.args.get("page", 1, type=int))
    size = min(100, max(1, request.args.get("size", 25, type=int)))
    sql = """SELECT o.*, CONCAT('NK', LPAD(o.id, 8, '0')) AS order_number,
             u.name AS customer_name, u.email AS customer_email, u.phone AS customer_phone,
             (SELECT COUNT(*) FROM order_items oi WHERE oi.order_id=o.id) AS item_count
             FROM orders o LEFT JOIN users u ON u.id=o.user_id WHERE 1=1"""
    params = []
    if status and status != "all":
        sql += " AND o.status = %s"; params.append(status)
    if search:
        like = f"%{search}%"
        sql += " AND (CONCAT('NK', LPAD(o.id, 8, '0')) LIKE %s OR u.name LIKE %s OR u.email LIKE %s OR u.phone LIKE %s OR EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id=o.id AND oi.product_name LIKE %s))"
        params.extend([like] * 5)
    sql += " ORDER BY o.created_at DESC LIMIT %s OFFSET %s"
    params.extend([size, (page - 1) * size])
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True); cursor.execute(sql, params); rows = cursor.fetchall()
        return jsonify({"orders": rows, "page": page, "size": size})
    finally:
        connection.close()


@app.get("/api/admin/orders/<order_number>")
@require_admin
def admin_order_detail(payload, order_number):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        order_id = order_id_from_number(order_number)
        cursor.execute("SELECT o.*, CONCAT('NK', LPAD(o.id, 8, '0')) AS order_number, u.name AS customer_name, u.email AS customer_email, u.phone AS customer_phone, u.created_at AS customer_created_at FROM orders o LEFT JOIN users u ON u.id=o.user_id WHERE o.id=%s", (order_id,))
        order = cursor.fetchone()
        if not order:
            return jsonify({"error": "Order not found."}), 404
        cursor.execute("SELECT oi.*, p.image_url FROM order_items oi LEFT JOIN products p ON p.id=oi.product_id WHERE oi.order_id=%s", (order_id,)); order["items"] = cursor.fetchall()
        cursor.execute("SELECT * FROM order_status_history WHERE order_id=%s ORDER BY created_at", (order_id,)); order["timeline"] = cursor.fetchall()
        cursor.execute("SELECT * FROM transactions WHERE order_id=%s ORDER BY created_at DESC", (order_id,)); order["transactions"] = cursor.fetchall()
        return jsonify({"order": order})
    finally:
        connection.close()


@app.patch("/api/admin/orders/<order_number>/payment")
@app.patch("/api/admin/orders/<order_number>/payment-status")
@require_admin
def admin_verify_upi_payment(payload, order_number):
    data = request.get_json(silent=True) or {}
    action = str(data.get("action") or data.get("status") or "").strip().upper()
    if action in {"VERIFY", "VERIFIED", "PAID", "COMPLETED"}:
        action = "VERIFY"
    elif action in {"REJECT", "REJECTED"}:
        action = "REJECT"
    else:
        return jsonify({"error": "Action must be VERIFY or REJECT."}), 400
    reason = str(data.get("reason") or data.get("rejection_reason") or "").strip()
    if len(reason) > 255:
        return jsonify({"error": "Rejection reason must be 255 characters or fewer."}), 400

    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        order_id = order_id_from_number(order_number)
        cursor.execute(
            "SELECT id, user_id, CONCAT('NK', LPAD(id, 8, '0')) AS order_number, status, "
            "payment_method, payment_status, total "
            "FROM orders WHERE id = %s FOR UPDATE",
            (order_id,),
        )
        order = cursor.fetchone()
        if not order:
            return jsonify({"error": "Order not found."}), 404
        if order["payment_method"] != "UPI":
            return jsonify({"error": "Only UPI payments require manual verification."}), 409
        cursor.execute(
            "SELECT * FROM transactions WHERE order_id = %s AND payment_method = 'UPI' "
            "ORDER BY created_at DESC LIMIT 1 FOR UPDATE",
            (order_id,),
        )
        transaction = cursor.fetchone()
        if not transaction or not transaction.get("utr"):
            return jsonify({"error": "No customer UTR has been submitted."}), 409
        target_status = "VERIFIED" if action == "VERIFY" else "REJECTED"
        if order["payment_status"] == target_status:
            return jsonify({"success": True, "payment_status": target_status, "changed": False})
        if order["payment_status"] in {"VERIFIED", "COMPLETED", "REJECTED"}:
            return jsonify({"error": "This payment has already been decided."}), 409

        admin_user_id = admin_id(payload)
        cursor.execute(
            "UPDATE transactions SET status = %s, verified_by = %s, verified_at = UTC_TIMESTAMP(), "
            "rejection_reason = %s WHERE id = %s",
            (target_status, admin_user_id, reason or None, transaction["id"]),
        )
        cursor.execute(
            "UPDATE orders SET payment_status = %s WHERE id = %s",
            (target_status, order_id),
        )
        if action == "VERIFY" and order["status"] in {"placed", "pending"}:
            cursor.execute("UPDATE orders SET status = 'confirmed' WHERE id = %s", (order_id,))
            cursor.execute(
                "INSERT INTO order_status_history (order_id, status, note) VALUES (%s, 'confirmed', %s)",
                (order_id, "UPI payment verified by admin"),
            )
        if order["user_id"]:
            if action == "VERIFY":
                title = "UPI payment verified"
                message = f"Your UPI payment has been verified and order {order['order_number']} is confirmed."
            else:
                title = "UPI payment rejected"
                message = f"Your UPI payment for order {order['order_number']} was rejected."
                if reason:
                    message += f" Reason: {reason}"
            create_notification(
                cursor, order["user_id"], title, message,
                "PAYMENT_" + target_status, order_id,
            )
        notify_admins(
            cursor,
            "UPI payment verified" if action == "VERIFY" else "UPI payment rejected",
            f"Order {order['order_number']} · UTR: {transaction['utr']}",
            "PAYMENT_" + target_status,
            order_id,
        )
        write_audit(
            cursor, payload, "UPI payment " + action.lower(), "payment", transaction["id"],
            {"status": order["payment_status"]}, {"status": target_status, "utr": transaction["utr"]},
        )
        connection.commit()
        return jsonify({
            "success": True, "payment_status": target_status,
            "order_status": "confirmed" if action == "VERIFY" and order["status"] in {"placed", "pending"} else order["status"],
        })
    finally:
        connection.close()


@app.get("/api/admin/products")
@require_admin
def admin_products(payload):
    search = (request.args.get("q") or "").strip()
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        sql = "SELECT p.*, c.name AS category_name, CASE WHEN p.stock_quantity=0 THEN 'OUT_OF_STOCK' WHEN p.stock_quantity<=p.low_stock_threshold THEN 'LOW_STOCK' WHEN p.is_active=0 THEN 'INACTIVE' ELSE 'ACTIVE' END AS inventory_status FROM products p LEFT JOIN categories c ON c.id=p.category_id"
        params = []
        if search:
            sql += " WHERE p.name LIKE %s OR p.sku LIKE %s OR p.slug LIKE %s OR p.brand LIKE %s OR p.subcategory LIKE %s OR c.name LIKE %s"; like = f"%{search}%"; params.extend([like] * 6)
        sql += " ORDER BY p.created_at DESC"
        cursor.execute(sql, params); return jsonify({"products": cursor.fetchall()})
    finally:
        connection.close()


@app.post("/api/admin/products")
@require_admin
def admin_create_product(payload):
    data = request.get_json(silent=True) or {}
    try:
        values = product_payload(data)
    except (ValueError, TypeError) as exc:
        return jsonify({"error": str(exc)}), 400
    if (not values.get("name") or values.get("price") is None
            or values.get("compare_at_price") is None
            or values.get("category_id") in (None, "")):
        return jsonify({"error": "Product name, price, MRP and category are required."}), 400
    slug = re.sub(r"[^a-z0-9]+", "-", str(values["name"]).lower()).strip("-") + "-" + secrets.token_hex(2)
    fields = ["slug"] + list(values.keys())
    connection = connect()
    try:
        cursor = connection.cursor()
        if values.get("category_id") is not None:
            try:
                values["category_id"] = int(values["category_id"])
            except (TypeError, ValueError):
                values["category_id"] = None
            if values["category_id"] is not None:
                cursor.execute("SELECT id FROM categories WHERE id = %s", (values["category_id"],))
                if not cursor.fetchone():
                    return jsonify({"error": "Category not found."}), 400
        if values.get("sku"):
            cursor.execute("SELECT id FROM products WHERE sku = %s", (values["sku"],))
            if cursor.fetchone():
                return jsonify({"error": "SKU already exists."}), 409
        params = [slug] + [values[field] for field in values]
        cursor.execute("INSERT INTO products (" + ",".join(fields) + ") VALUES (" + ",".join(["%s"] * len(fields)) + ")", params)
        product_id = cursor.lastrowid
        if values.get("image_url"):
            cursor.execute(
                "INSERT INTO product_images (product_id,image_url,is_primary,sort_order) VALUES (%s,%s,1,0)",
                (product_id, values["image_url"]),
            )
        write_audit(cursor, payload, "Product created", "product", product_id, None, values)
        connection.commit()
        return jsonify({"id": product_id, "slug": slug}), 201
    finally:
        connection.close()


@app.post("/api/admin/uploads/product-image")
@require_admin
def admin_upload_product_image(payload):
    image = request.files.get("image")
    if not image or not image.filename:
        return jsonify({"error": "Select an image to upload."}), 400
    allowed = {"jpg", "jpeg", "png", "webp"}
    extension = Path(secure_filename(image.filename)).suffix.lower().lstrip(".")
    if extension not in allowed or (image.mimetype or "").lower() not in {
        "image/jpeg", "image/png", "image/webp"
    }:
        return jsonify({"error": "Only JPG, JPEG, PNG and WEBP images are allowed."}), 400
    image.stream.seek(0, os.SEEK_END)
    size = image.stream.tell()
    image.stream.seek(0)
    if size > 5 * 1024 * 1024:
        return jsonify({"error": "Images must be 5 MB or smaller."}), 400
    signature = image.stream.read(12)
    image.stream.seek(0)
    valid_signature = (
        (extension in {"jpg", "jpeg"} and signature.startswith(b"\xff\xd8\xff"))
        or (extension == "png" and signature.startswith(b"\x89PNG\r\n\x1a\n"))
        or (extension == "webp" and signature.startswith(b"RIFF") and signature[8:12] == b"WEBP")
    )
    if not valid_signature:
        return jsonify({"error": "The uploaded file is not a valid image."}), 400
    filename = f"{uuid.uuid4().hex}.{extension}"
    image.save(UPLOAD_DIR / filename)
    return jsonify({"url": f"{request.host_url.rstrip('/')}/uploads/{filename}", "filename": filename}), 201


@app.get("/api/admin/products/<int:product_id>/images")
@require_admin
def admin_product_images(payload, product_id):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            "SELECT id, image_url, alt_text, sort_order, is_primary FROM product_images "
            "WHERE product_id=%s ORDER BY sort_order,id", (product_id,)
        )
        return jsonify({"images": cursor.fetchall()})
    finally:
        connection.close()


@app.post("/api/admin/products/<int:product_id>/images")
@require_admin
def admin_add_product_image(payload, product_id):
    data = request.get_json(silent=True) or {}
    image_url = str(data.get("image_url") or "").strip()
    if not image_url:
        return jsonify({"error": "Image URL is required."}), 400
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT id FROM products WHERE id=%s", (product_id,))
        if not cursor.fetchone():
            return jsonify({"error": "Product not found."}), 404
        cursor.execute("SELECT COALESCE(MAX(sort_order), -1) + 1 FROM product_images WHERE product_id=%s", (product_id,))
        sort_order = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO product_images (product_id,image_url,alt_text,sort_order,is_primary) "
            "VALUES (%s,%s,%s,%s,%s)",
            (product_id, image_url, data.get("alt_text"), sort_order, bool(data.get("is_primary"))),
        )
        connection.commit()
        return jsonify({"id": cursor.lastrowid}), 201
    finally:
        connection.close()


@app.delete("/api/admin/products/<int:product_id>/images/<int:image_id>")
@require_admin
def admin_delete_product_image(payload, product_id, image_id):
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM product_images WHERE id=%s AND product_id=%s", (image_id, product_id))
        if cursor.rowcount == 0:
            return jsonify({"error": "Image not found."}), 404
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.post("/api/admin/uploads/upi-qr")
@require_admin
def admin_upload_upi_qr(payload):
    image = request.files.get("image")
    if not image or not image.filename:
        return jsonify({"error": "Select a QR image to upload."}), 400
    allowed = {"jpg", "jpeg", "png", "webp"}
    extension = Path(secure_filename(image.filename)).suffix.lower().lstrip(".")
    if extension not in allowed or (image.mimetype or "").lower() not in {
        "image/jpeg", "image/png", "image/webp"
    }:
        return jsonify({"error": "Only JPG, JPEG, PNG and WEBP images are allowed."}), 400
    image.stream.seek(0, os.SEEK_END)
    size = image.stream.tell()
    image.stream.seek(0)
    if size > 5 * 1024 * 1024:
        return jsonify({"error": "QR images must be 5 MB or smaller."}), 400
    signature = image.stream.read(12)
    image.stream.seek(0)
    valid_signature = (
        (extension in {"jpg", "jpeg"} and signature.startswith(b"\xff\xd8\xff"))
        or (extension == "png" and signature.startswith(b"\x89PNG\r\n\x1a\n"))
        or (extension == "webp" and signature.startswith(b"RIFF") and signature[8:12] == b"WEBP")
    )
    if not valid_signature:
        return jsonify({"error": "The uploaded file is not a valid image."}), 400
    filename = f"upi-qr-{uuid.uuid4().hex}.{extension}"
    image.save(UPLOAD_DIR / filename)
    url = f"{request.host_url.rstrip('/')}/uploads/{filename}"
    connection = connect()
    previous_url = None
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT setting_value FROM settings WHERE setting_key = 'upi_qr_url'")
        previous = cursor.fetchone()
        previous_url = str((previous or {}).get("setting_value") or "")
        cursor.execute(
            "INSERT INTO settings (setting_key, setting_value) VALUES ('upi_qr_url', %s) "
            "ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value)",
            (url,),
        )
        write_audit(cursor, payload, "UPI QR uploaded", "settings", "upi_qr_url", previous, url)
        connection.commit()
    finally:
        connection.close()
    if previous_url:
        previous_name = Path(previous_url.split("?")[0]).name
        if previous_name.startswith("upi-qr-") and previous_name != filename:
            try:
                (UPLOAD_DIR / previous_name).unlink(missing_ok=True)
            except OSError:
                pass
    return jsonify({"success": True, "url": url, "filename": filename}), 201


@app.delete("/api/admin/uploads/upi-qr")
@require_admin
def admin_remove_upi_qr(payload):
    connection = connect()
    old_url = None
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT setting_value FROM settings WHERE setting_key = 'upi_qr_url'")
        previous = cursor.fetchone()
        old_url = str((previous or {}).get("setting_value") or "")
        cursor.execute(
            "INSERT INTO settings (setting_key, setting_value) VALUES ('upi_qr_url', '') "
            "ON DUPLICATE KEY UPDATE setting_value = ''"
        )
        write_audit(cursor, payload, "UPI QR removed", "settings", "upi_qr_url", previous, None)
        connection.commit()
    finally:
        connection.close()
    if old_url:
        old_name = Path(old_url.split("?")[0]).name
        if old_name.startswith("upi-qr-"):
            try:
                (UPLOAD_DIR / old_name).unlink(missing_ok=True)
            except OSError:
                pass
    return jsonify({"success": True})


@app.post("/api/admin/products/bulk")
@require_admin
def admin_bulk_products(payload):
    data = request.get_json(silent=True) or {}
    ids = data.get("product_ids") or []
    action = str(data.get("action") or "").lower()
    if not ids or action not in {"activate", "deactivate", "archive", "category", "stock", "price"}:
        return jsonify({"error": "Choose products and a valid bulk action."}), 400
    try:
        ids = [int(item) for item in ids]
    except (TypeError, ValueError):
        return jsonify({"error": "Product IDs must be numbers."}), 400
    connection = connect()
    try:
        cursor = connection.cursor()
        placeholders = ",".join(["%s"] * len(ids))
        params = list(ids)
        if action in {"activate", "deactivate", "archive"}:
            active = action == "activate"
            cursor.execute(f"UPDATE products SET is_active=%s WHERE id IN ({placeholders})", [active] + params)
        elif action == "category":
            category_id = data.get("category_id")
            if not category_id:
                return jsonify({"error": "Category is required."}), 400
            cursor.execute("SELECT id FROM categories WHERE id=%s AND is_active=1", (category_id,))
            if not cursor.fetchone():
                return jsonify({"error": "Category not found."}), 400
            cursor.execute(f"UPDATE products SET category_id=%s WHERE id IN ({placeholders})", [category_id] + params)
        elif action == "stock":
            value = int(data.get("stock_quantity"))
            if value < 0:
                raise ValueError
            cursor.execute(f"UPDATE products SET stock_quantity=%s WHERE id IN ({placeholders})", [value] + params)
        else:
            value = Decimal(str(data.get("price")))
            if value <= 0:
                raise ValueError
            cursor.execute(f"UPDATE products SET price=%s WHERE id IN ({placeholders})", [value] + params)
        write_audit(cursor, payload, f"Bulk product {action}", "product", ",".join(map(str, ids)), None, data)
        connection.commit()
        return jsonify({"success": True, "updated": cursor.rowcount})
    except (TypeError, ValueError):
        connection.rollback()
        return jsonify({"error": "Bulk value is invalid."}), 400
    finally:
        connection.close()


@app.post("/api/admin/products/import")
@require_admin
def admin_import_products(payload):
    upload = request.files.get("file")
    if not upload or not upload.filename:
        return jsonify({"error": "Upload a CSV or XLSX file."}), 400
    extension = Path(secure_filename(upload.filename)).suffix.lower()
    if extension == ".xlsx":
        try:
            from openpyxl import load_workbook
        except ImportError:
            return jsonify({"error": "XLSX import requires the openpyxl package; use CSV or install it."}), 400
        rows = list(load_workbook(upload, read_only=True, data_only=True).active.values)
        if not rows:
            return jsonify({"error": "The spreadsheet is empty."}), 400
        headers, raw_rows = [str(x or "").strip().lower() for x in rows[0]], rows[1:]
        records = [dict(zip(headers, row)) for row in raw_rows]
    elif extension == ".csv":
        try:
            text = upload.read().decode("utf-8-sig")
            records = list(csv.DictReader(io.StringIO(text)))
        except (UnicodeDecodeError, csv.Error):
            return jsonify({"error": "CSV must be valid UTF-8 with a header row."}), 400
    else:
        return jsonify({"error": "Only CSV and XLSX files are supported."}), 400
    required = {"name", "sku", "price", "compare_at_price", "stock_quantity"}
    headers = {str(k).strip().lower() for k in (records[0].keys() if records else [])}
    missing = sorted(required - headers)
    if "category_id" not in headers and "category" not in headers and "category_slug" not in headers:
        missing.append("category (or category_id)")
    if missing:
        return jsonify({"imported": len(records), "successful": 0, "failed": len(records),
                        "errors": [{"row": 1, "error": f"Missing columns: {', '.join(missing)}"}]}), 400
    connection = connect()
    successful, errors = 0, []
    try:
        cursor = connection.cursor(dictionary=True)
        for row_number, row in enumerate(records, 2):
            normalized = {str(k).strip().lower(): v for k, v in row.items()}
            try:
                values = product_payload(normalized)
                if not values.get("category_id") and normalized.get("category"):
                    cursor.execute("SELECT id FROM categories WHERE slug=%s OR name=%s",
                                   (str(normalized["category"]).strip().lower(), str(normalized["category"]).strip()))
                    category = cursor.fetchone()
                    values["category_id"] = category["id"] if category else None
                if not values.get("category_id") and normalized.get("category_slug"):
                    cursor.execute("SELECT id FROM categories WHERE slug=%s",
                                   (str(normalized["category_slug"]).strip().lower(),))
                    category = cursor.fetchone()
                    values["category_id"] = category["id"] if category else None
                if not values.get("name") or not values.get("category_id"):
                    raise ValueError("Name and category are required.")
                values["category_id"] = int(values["category_id"])
                cursor.execute("SELECT id FROM categories WHERE id=%s AND is_active=1", (values["category_id"],))
                if not cursor.fetchone():
                    raise ValueError("Invalid category.")
                if not values.get("sku"):
                    raise ValueError("SKU is required.")
                cursor.execute("SELECT id FROM products WHERE sku=%s", (values["sku"],))
                if cursor.fetchone():
                    raise ValueError("SKU already exists.")
                slug = re.sub(r"[^a-z0-9]+", "-", str(values["name"]).lower()).strip("-") + "-" + secrets.token_hex(2)
                fields = ["slug"] + list(values.keys())
                cursor.execute("INSERT INTO products (" + ",".join(fields) + ") VALUES (" +
                               ",".join(["%s"] * len(fields)) + ")",
                               [slug] + [values[field] for field in values])
                successful += 1
            except (ValueError, TypeError, Error) as exc:
                errors.append({"row": row_number, "error": str(exc)})
        connection.commit()
        return jsonify({"imported": len(records), "successful": successful,
                        "failed": len(errors), "errors": errors})
    finally:
        connection.close()


@app.put("/api/admin/products/<int:product_id>")
@require_admin
def admin_update_product(payload, product_id):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True); cursor.execute("SELECT * FROM products WHERE id=%s", (product_id,)); existing = cursor.fetchone()
        if not existing: return jsonify({"error": "Product not found."}), 404
        try: values = product_payload(request.get_json(silent=True) or {}, existing)
        except (ValueError, TypeError) as exc: return jsonify({"error": str(exc)}), 400
        if not values: return jsonify({"error": "No changes supplied."}), 400
        sql = "UPDATE products SET " + ",".join(f"{key}=%s" for key in values) + " WHERE id=%s"
        cursor.execute(sql, list(values.values()) + [product_id])
        if values.get("image_url") and values.get("image_url") != existing.get("image_url"):
            cursor.execute(
                "INSERT INTO product_images (product_id,image_url,is_primary,sort_order) VALUES (%s,%s,1,0)",
                (product_id, values["image_url"]),
            )
        write_audit(cursor, payload, "Product updated", "product", product_id, existing, values)
        connection.commit(); return jsonify({"success": True})
    finally:
        connection.close()


@app.delete("/api/admin/products/<int:product_id>")
@require_admin
def admin_delete_product(payload, product_id):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True); cursor.execute("SELECT name FROM products WHERE id=%s", (product_id,)); item = cursor.fetchone()
        if not item: return jsonify({"error": "Product not found."}), 404
        cursor.execute("UPDATE products SET is_active=0 WHERE id=%s", (product_id,))
        write_audit(cursor, payload, "Product archived", "product", product_id, item, {"is_active": False}); connection.commit()
        return jsonify({"success": True})
    finally: connection.close()


@app.patch("/api/admin/products/<int:product_id>/status")
@require_admin
def admin_product_status(payload, product_id):
    active = bool((request.get_json(silent=True) or {}).get("is_active"))
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("UPDATE products SET is_active=%s WHERE id=%s", (active, product_id))
        if cursor.rowcount == 0:
            return jsonify({"error": "Product not found."}), 404
        write_audit(cursor, payload, "Product status changed", "product", product_id, None, {"is_active": active})
        connection.commit()
        return jsonify({"success": True, "is_active": active})
    finally:
        connection.close()


@app.post("/api/admin/products/<int:product_id>/duplicate")
@require_admin
def admin_duplicate_product(payload, product_id):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM products WHERE id=%s", (product_id,))
        product = cursor.fetchone()
        if not product:
            return jsonify({"error": "Product not found."}), 404
        slug = f"{product['slug']}-copy-{secrets.token_hex(2)}"
        cursor.execute(
            "INSERT INTO products (category_id,name,slug,description,price,compare_at_price,image_url,stock_quantity,is_active,sku,short_description,brand,subcategory,low_stock_threshold,weight,dimensions,material,color,size,capacity,tags,is_featured,is_bestseller,is_new_arrival,is_trending) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,0,0,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,0,0,0)",
            (product.get("category_id"), f"{product['name']} (Copy)", slug, product.get("description"), product["price"],
             product.get("compare_at_price"), product.get("image_url"), product.get("sku"), product.get("short_description"),
             product.get("brand"), product.get("subcategory"), product.get("low_stock_threshold", 5), product.get("weight"),
             product.get("dimensions"), product.get("material"), product.get("color"), product.get("size"), product.get("capacity"),
             product.get("tags")),
        )
        new_id = cursor.lastrowid
        write_audit(cursor, payload, "Product duplicated", "product", new_id, {"source_id": product_id}, {"name": f"{product['name']} (Copy)"})
        connection.commit()
        return jsonify({"id": new_id, "slug": slug}), 201
    finally:
        connection.close()


@app.get("/api/admin/categories")
@require_admin
def admin_categories(payload):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True); cursor.execute("SELECT c.*, COUNT(p.id) AS product_count FROM categories c LEFT JOIN products p ON p.category_id=c.id GROUP BY c.id ORDER BY c.name"); return jsonify({"categories": cursor.fetchall()})
    finally: connection.close()


@app.post("/api/admin/categories")
@require_admin
def admin_create_category(payload):
    data = request.get_json(silent=True) or {}; name = str(data.get("name") or "").strip()
    if not name: return jsonify({"error": "Category name is required."}), 400
    slug = re.sub(r"[^a-z0-9]+", "-", str(data.get("slug") or name).lower()).strip("-")
    connection = connect()
    try:
        cursor = connection.cursor()
        parent_id = data.get("parent_id") or None
        cursor.execute(
            "INSERT INTO categories (name,slug,description,icon,parent_id,is_active) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (name, slug, data.get("description"), data.get("icon"), parent_id,
             bool(data.get("is_active", True))),
        )
        write_audit(cursor,payload,"Category created","category",cursor.lastrowid,None,data)
        connection.commit()
        return jsonify({"id":cursor.lastrowid,"slug":slug}),201
    finally: connection.close()


@app.put("/api/admin/categories/<int:category_id>")
@require_admin
def admin_update_category(payload, category_id):
    data = request.get_json(silent=True) or {}; allowed = {k: data[k] for k in ("name","slug","description","icon","parent_id","is_active") if k in data}
    if not allowed: return jsonify({"error":"No changes supplied."}),400
    connection = connect()
    try:
        cursor=connection.cursor(dictionary=True); cursor.execute("SELECT * FROM categories WHERE id=%s",(category_id,)); old=cursor.fetchone()
        if not old:return jsonify({"error":"Category not found."}),404
        cursor.execute("UPDATE categories SET "+",".join(f"{k}=%s" for k in allowed)+" WHERE id=%s",list(allowed.values())+[category_id]); write_audit(cursor,payload,"Category updated","category",category_id,old,allowed); connection.commit(); return jsonify({"success":True})
    finally: connection.close()


@app.delete("/api/admin/categories/<int:category_id>")
@require_admin
def admin_delete_category(payload, category_id):
    connection=connect()
    try:
        cursor=connection.cursor(); cursor.execute("DELETE FROM categories WHERE id=%s",(category_id,)); write_audit(cursor,payload,"Category deleted","category",category_id); connection.commit(); return jsonify({"success":True})
    finally: connection.close()


@app.get("/api/admin/inventory")
@require_admin
def admin_inventory(payload):
    connection=connect()
    try:
        cursor=connection.cursor(dictionary=True); cursor.execute("SELECT p.id,p.sku,p.name,p.stock_quantity,p.low_stock_threshold,p.is_active,p.updated_at, CASE WHEN p.stock_quantity=0 THEN 'OUT_OF_STOCK' WHEN p.stock_quantity<=p.low_stock_threshold THEN 'LOW_STOCK' WHEN p.is_active=0 THEN 'INACTIVE' ELSE 'IN_STOCK' END AS status FROM products p ORDER BY status,p.name"); return jsonify({"inventory":cursor.fetchall()})
    finally: connection.close()


@app.patch("/api/admin/inventory/<int:product_id>")
@require_admin
def admin_adjust_inventory(payload, product_id):
    data=request.get_json(silent=True) or {}; operation=str(data.get("operation") or "set").lower(); reason=str(data.get("reason") or "").strip()
    if not reason:return jsonify({"error":"A reason is required for stock changes."}),400
    try: amount=int(data.get("quantity"))
    except (TypeError,ValueError):return jsonify({"error":"Quantity must be a whole number."}),400
    connection=connect()
    try:
        cursor=connection.cursor(dictionary=True); cursor.execute("SELECT stock_quantity, low_stock_threshold FROM products WHERE id=%s FOR UPDATE",(product_id,)); row=cursor.fetchone()
        if not row:return jsonify({"error":"Product not found."}),404
        previous=int(row["stock_quantity"]); new=amount if operation=="set" else previous + amount if operation=="add" else previous - amount
        if operation not in {"set","add","remove"} or new<0:return jsonify({"error":"Invalid stock adjustment."}),400
        cursor.execute("UPDATE products SET stock_quantity=%s WHERE id=%s",(new,product_id)); cursor.execute("INSERT INTO inventory_movements (product_id,previous_quantity,new_quantity,difference,reason,admin_id) VALUES (%s,%s,%s,%s,%s,%s)",(product_id,previous,new,new-previous,reason,admin_id(payload))); write_audit(cursor,payload,"Stock updated","product",product_id,{"stock_quantity":previous},{"stock_quantity":new,"reason":reason})
        if new == 0:
            notify_admins(cursor, "Out of stock", f"Product {product_id} is out of stock.", "OUT_OF_STOCK")
        elif new <= int(row.get("low_stock_threshold") or 0):
            notify_admins(cursor, "Low stock", f"Product {product_id} has reached its stock threshold.", "LOW_STOCK")
        connection.commit(); return jsonify({"success":True,"previous_quantity":previous,"new_quantity":new})
    finally: connection.close()


@app.get("/api/admin/inventory/<int:product_id>/history")
@require_admin
def admin_inventory_history(payload, product_id):
    connection=connect()
    try:
        cursor=connection.cursor(dictionary=True); cursor.execute("SELECT im.*,u.name AS admin_name FROM inventory_movements im LEFT JOIN users u ON u.id=im.admin_id WHERE im.product_id=%s ORDER BY im.created_at DESC LIMIT 100",(product_id,)); return jsonify({"history":cursor.fetchall()})
    finally: connection.close()


@app.get("/api/admin/customers")
@require_admin
def admin_customers(payload):
    search=(request.args.get("q") or "").strip(); sql="""SELECT u.id,u.name,u.email,u.phone,u.role,u.is_active,u.created_at,u.last_login,
      COUNT(DISTINCT o.id) AS total_orders,COALESCE(SUM(o.total),0) AS total_spent,MAX(o.created_at) AS last_order
      FROM users u LEFT JOIN orders o ON o.user_id=u.id WHERE u.role='CUSTOMER'"""; params=[]
    if search:sql+=" AND (u.name LIKE %s OR u.email LIKE %s OR u.phone LIKE %s OR CAST(u.id AS CHAR) LIKE %s)"; params.extend([f"%{search}%"]*4)
    sql+=" GROUP BY u.id ORDER BY u.created_at DESC"; connection=connect()
    try: cursor=connection.cursor(dictionary=True);cursor.execute(sql,params);return jsonify({"customers":cursor.fetchall()})
    finally: connection.close()


@app.get("/api/admin/customers/<int:customer_id>")
@require_admin
def admin_customer_detail(payload, customer_id):
    connection=connect()
    try:
        cursor=connection.cursor(dictionary=True);cursor.execute("SELECT id,name,email,phone,role,created_at,last_login FROM users WHERE id=%s AND role='CUSTOMER'",(customer_id,)); user=cursor.fetchone()
        if not user:return jsonify({"error":"Customer not found."}),404
        cursor.execute("SELECT * FROM addresses WHERE user_id=%s ORDER BY is_default DESC",(customer_id,)); user["addresses"]=cursor.fetchall()
        cursor.execute("SELECT o.*,CONCAT('NK',LPAD(o.id,8,'0')) AS order_number FROM orders o WHERE user_id=%s ORDER BY created_at DESC",(customer_id,)); user["orders"]=cursor.fetchall()
        cursor.execute("SELECT * FROM transactions WHERE user_id=%s ORDER BY created_at DESC",(customer_id,)); user["transactions"]=cursor.fetchall()
        cursor.execute("SELECT COUNT(*) AS total FROM wishlists WHERE user_id=%s",(customer_id,)); user["wishlist_count"]=cursor.fetchone()["total"]; return jsonify({"customer":user})
    finally: connection.close()


@app.patch("/api/admin/customers/<int:customer_id>/status")
@require_admin
def admin_customer_status(payload, customer_id):
    active = bool((request.get_json(silent=True) or {}).get("is_active"))
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("UPDATE users SET is_active=%s WHERE id=%s AND role='CUSTOMER'", (active, customer_id))
        if cursor.rowcount == 0:
            return jsonify({"error": "Customer not found."}), 404
        write_audit(cursor, payload, "Customer status changed", "customer", customer_id, None, {"is_active": active})
        connection.commit()
        return jsonify({"success": True, "is_active": active})
    finally:
        connection.close()


@app.get("/api/admin/transactions")
@require_admin
def admin_transactions(payload):
    connection=connect()
    try:
        cursor=connection.cursor(dictionary=True);cursor.execute("""SELECT t.*,CONCAT('NK',LPAD(t.order_id,8,'0')) AS order_number,u.name AS customer_name FROM transactions t LEFT JOIN users u ON u.id=t.user_id ORDER BY t.created_at DESC LIMIT 500""");return jsonify({"transactions":cursor.fetchall()})
    finally: connection.close()


@app.get("/api/admin/enquiries")
@require_admin
def admin_enquiries(payload):
    connection=connect()
    try: cursor=connection.cursor(dictionary=True);cursor.execute("SELECT * FROM enquiries ORDER BY created_at DESC");return jsonify({"enquiries":cursor.fetchall()})
    finally: connection.close()


@app.patch("/api/admin/enquiries/<int:enquiry_id>")
@require_admin
def admin_update_enquiry(payload,enquiry_id):
    status=str((request.get_json(silent=True) or {}).get("status") or "").upper()
    if status not in {"NEW","IN_PROGRESS","RESOLVED","CLOSED"}:return jsonify({"error":"Unsupported enquiry status."}),400
    connection=connect()
    try: cursor=connection.cursor();cursor.execute("UPDATE enquiries SET status=%s WHERE id=%s",(status.lower(),enquiry_id));write_audit(cursor,payload,"Enquiry status changed","enquiry",enquiry_id,None,status);connection.commit();return jsonify({"success":True,"status":status})
    finally:connection.close()


@app.get("/api/admin/banners")
@require_admin
def admin_banners(payload):
    connection=connect()
    try:cursor=connection.cursor(dictionary=True);cursor.execute("SELECT * FROM banners ORDER BY priority DESC,created_at DESC");return jsonify({"banners":cursor.fetchall()})
    finally:connection.close()


@app.post("/api/admin/banners")
@require_admin
def admin_create_banner(payload):
    data=request.get_json(silent=True) or {}; title=str(data.get("title") or "").strip()
    if not title:return jsonify({"error":"Banner title is required."}),400
    fields=("title","subtitle","image_url","link_url","cta_text","target_type","banner_type","priority","start_date","end_date","is_active")
    values=[data.get(k) for k in fields]
    values[7] = int(data.get("priority") or 0)
    values[10] = bool(data.get("is_active", True))
    connection=connect()
    try:cursor=connection.cursor();cursor.execute("INSERT INTO banners ("+",".join(fields)+") VALUES ("+",".join(["%s"]*len(fields))+")",values);write_audit(cursor,payload,"Banner created","banner",cursor.lastrowid,None,data);connection.commit();return jsonify({"id":cursor.lastrowid}),201
    finally:connection.close()


@app.put("/api/admin/banners/<int:banner_id>")
@require_admin
def admin_update_banner(payload,banner_id):
    data=request.get_json(silent=True) or {}; allowed={k:data[k] for k in ("title","subtitle","image_url","link_url","cta_text","target_type","banner_type","priority","start_date","end_date","is_active") if k in data}
    if not allowed:return jsonify({"error":"No changes supplied."}),400
    connection=connect()
    try:cursor=connection.cursor();cursor.execute("UPDATE banners SET "+",".join(f"{k}=%s" for k in allowed)+" WHERE id=%s",list(allowed.values())+[banner_id]);write_audit(cursor,payload,"Banner updated","banner",banner_id,None,allowed);connection.commit();return jsonify({"success":True})
    finally:connection.close()


@app.delete("/api/admin/banners/<int:banner_id>")
@require_admin
def admin_delete_banner(payload,banner_id):
    connection=connect()
    try:cursor=connection.cursor();cursor.execute("DELETE FROM banners WHERE id=%s",(banner_id,));write_audit(cursor,payload,"Banner deleted","banner",banner_id);connection.commit();return jsonify({"success":True})
    finally:connection.close()


@app.get("/api/admin/offers")
@require_admin
def admin_offers(payload):
    connection=connect()
    try:cursor=connection.cursor(dictionary=True);cursor.execute("SELECT * FROM offers ORDER BY created_at DESC");return jsonify({"offers":cursor.fetchall()})
    finally:connection.close()


@app.post("/api/admin/offers")
@require_admin
def admin_create_offer(payload):
    data=request.get_json(silent=True) or {}; name=str(data.get("name") or "").strip()
    if not name:return jsonify({"error":"Offer name is required."}),400
    fields=("name","code","discount_type","value","start_date","end_date","usage_limit","status","applicable_product_ids","applicable_category_ids")
    values=[data.get(k) for k in fields]
    values[2] = data.get("discount_type") or "PERCENTAGE"
    values[3] = data.get("value") or 0
    values[7] = data.get("status") or "ACTIVE"
    if values[1] == "":
        values[1] = None
    connection=connect()
    try:cursor=connection.cursor();cursor.execute("INSERT INTO offers ("+",".join(fields)+") VALUES ("+",".join(["%s"]*len(fields))+")",values);write_audit(cursor,payload,"Offer created","offer",cursor.lastrowid,None,data);connection.commit();return jsonify({"id":cursor.lastrowid}),201
    finally:connection.close()


@app.put("/api/admin/offers/<int:offer_id>")
@require_admin
def admin_update_offer(payload,offer_id):
    data=request.get_json(silent=True) or {}; allowed={k:data[k] for k in ("name","code","discount_type","value","start_date","end_date","usage_limit","status","applicable_product_ids","applicable_category_ids") if k in data}
    if not allowed:return jsonify({"error":"No changes supplied."}),400
    connection=connect()
    try:cursor=connection.cursor();cursor.execute("UPDATE offers SET "+",".join(f"{k}=%s" for k in allowed)+" WHERE id=%s",list(allowed.values())+[offer_id]);write_audit(cursor,payload,"Offer updated","offer",offer_id,None,allowed);connection.commit();return jsonify({"success":True})
    finally:connection.close()


@app.delete("/api/admin/offers/<int:offer_id>")
@require_admin
def admin_delete_offer(payload,offer_id):
    connection=connect()
    try:cursor=connection.cursor();cursor.execute("DELETE FROM offers WHERE id=%s",(offer_id,));write_audit(cursor,payload,"Offer deleted","offer",offer_id);connection.commit();return jsonify({"success":True})
    finally:connection.close()


@app.get("/api/admin/campaigns")
@require_admin
def admin_campaigns(payload):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            """SELECT c.*, u.name AS created_by_name,
                      COUNT(cp.product_id) AS product_count
               FROM campaigns c LEFT JOIN users u ON u.id=c.created_by
               LEFT JOIN campaign_products cp ON cp.campaign_id=c.id
               GROUP BY c.id ORDER BY c.created_at DESC"""
        )
        return jsonify({"campaigns": cursor.fetchall()})
    finally:
        connection.close()


def _campaign_values(data, existing=None):
    existing = existing or {}
    name = str(data.get("name", existing.get("name", "")) or "").strip()
    if not name:
        raise ValueError("Campaign name is required.")
    slug = re.sub(r"[^a-z0-9]+", "-", str(data.get("slug", existing.get("slug", name))).lower()).strip("-")
    if not slug:
        raise ValueError("Campaign slug is required.")
    status = str(data.get("status", existing.get("status", "DRAFT")) or "DRAFT").upper()
    if status not in {"DRAFT", "ACTIVE", "PAUSED", "ENDED"}:
        raise ValueError("Unsupported campaign status.")
    try:
        budget = data.get("budget", existing.get("budget"))
        budget = None if budget in (None, "") else Decimal(str(budget))
        if budget is not None and budget < 0:
            raise ValueError
    except (TypeError, ValueError):
        raise ValueError("Budget must be a non-negative number.")
    return {
        "name": name, "slug": slug, "description": data.get("description", existing.get("description")),
        "campaign_type": str(data.get("campaign_type", existing.get("campaign_type", "PROMOTION"))).upper(),
        "banner_image_url": data.get("banner_image_url", existing.get("banner_image_url")),
        "landing_url": data.get("landing_url", existing.get("landing_url")),
        "start_date": data.get("start_date", existing.get("start_date")),
        "end_date": data.get("end_date", existing.get("end_date")),
        "status": status, "budget": budget,
    }


@app.post("/api/admin/campaigns")
@require_admin
def admin_create_campaign(payload):
    data = request.get_json(silent=True) or {}
    try:
        values = _campaign_values(data)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    product_ids = data.get("product_ids") or []
    connection = connect()
    try:
        cursor = connection.cursor()
        fields = list(values)
        cursor.execute(
            "INSERT INTO campaigns (" + ",".join(fields) + ",created_by) VALUES (" +
            ",".join(["%s"] * (len(fields) + 1)) + ")",
            [values[field] for field in fields] + [admin_id(payload)],
        )
        campaign_id = cursor.lastrowid
        for product_id in product_ids:
            try:
                cursor.execute(
                    "INSERT IGNORE INTO campaign_products (campaign_id, product_id) "
                    "SELECT %s, id FROM products WHERE id=%s AND is_active=1",
                    (campaign_id, int(product_id)),
                )
            except (TypeError, ValueError):
                continue
        write_audit(cursor, payload, "Campaign created", "campaign", campaign_id, None, values)
        connection.commit()
        return jsonify({"id": campaign_id, "slug": values["slug"]}), 201
    except Error as exc:
        connection.rollback()
        if getattr(exc, "errno", None) == 1062:
            return jsonify({"error": "Campaign slug already exists."}), 409
        raise
    finally:
        connection.close()


@app.put("/api/admin/campaigns/<int:campaign_id>")
@require_admin
def admin_update_campaign(payload, campaign_id):
    data = request.get_json(silent=True) or {}
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
        existing = cursor.fetchone()
        if not existing:
            return jsonify({"error": "Campaign not found."}), 404
        try:
            values = _campaign_values(data, existing)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        assignments = ",".join(f"{key}=%s" for key in values)
        cursor.execute("UPDATE campaigns SET " + assignments + " WHERE id=%s",
                       list(values.values()) + [campaign_id])
        if "product_ids" in data:
            cursor.execute("DELETE FROM campaign_products WHERE campaign_id=%s", (campaign_id,))
            for product_id in data.get("product_ids") or []:
                try:
                    cursor.execute(
                        "INSERT IGNORE INTO campaign_products (campaign_id, product_id) "
                        "SELECT %s,id FROM products WHERE id=%s AND is_active=1",
                        (campaign_id, int(product_id)),
                    )
                except (TypeError, ValueError):
                    continue
        write_audit(cursor, payload, "Campaign updated", "campaign", campaign_id, existing, values)
        connection.commit()
        return jsonify({"success": True})
    except Error as exc:
        connection.rollback()
        if getattr(exc, "errno", None) == 1062:
            return jsonify({"error": "Campaign slug already exists."}), 409
        raise
    finally:
        connection.close()


@app.delete("/api/admin/campaigns/<int:campaign_id>")
@require_admin
def admin_delete_campaign(payload, campaign_id):
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM campaigns WHERE id=%s", (campaign_id,))
        if cursor.rowcount == 0:
            return jsonify({"error": "Campaign not found."}), 404
        write_audit(cursor, payload, "Campaign deleted", "campaign", campaign_id)
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


@app.get("/api/admin/notifications")
@require_admin
def admin_notifications(payload):
    connection=connect()
    try:cursor=connection.cursor(dictionary=True);    cursor.execute(
        "SELECT n.*, COALESCE(n.order_number, CONCAT('NK', LPAD(n.order_id, 8, '0'))) AS order_number "
        "FROM notifications n WHERE n.user_id=%s ORDER BY n.created_at DESC LIMIT 200",
        (admin_id(payload),),
    );rows=cursor.fetchall();return jsonify({"notifications":rows,"unread_count":sum(not r["is_read"] for r in rows)})
    finally:connection.close()


@app.post("/api/admin/notifications/<int:notification_id>/read")
@require_admin
def admin_notification_read(payload,notification_id):
    connection=connect()
    try:cursor=connection.cursor();cursor.execute("UPDATE notifications SET is_read=1 WHERE id=%s AND user_id=%s",(notification_id,admin_id(payload)));connection.commit();return jsonify({"success":True})
    finally:connection.close()


@app.get("/api/admin/audit-logs")
@require_admin
def admin_audit_logs(payload):
    connection=connect()
    try:cursor=connection.cursor(dictionary=True);cursor.execute("SELECT a.*,u.name AS admin_name FROM audit_logs a LEFT JOIN users u ON u.id=a.admin_id ORDER BY a.created_at DESC LIMIT 500");return jsonify({"logs":cursor.fetchall()})
    finally:connection.close()


@app.get("/api/admin/analytics")
@require_admin
def admin_analytics(payload):
    connection=connect()
    try:
        cursor=connection.cursor(dictionary=True); cursor.execute("SELECT DATE(created_at) AS day,COUNT(*) AS orders,COALESCE(SUM(total),0) AS revenue FROM orders WHERE created_at>=DATE_SUB(CURDATE(),INTERVAL 30 DAY) GROUP BY DATE(created_at) ORDER BY day");daily=cursor.fetchall()
        cursor.execute("SELECT oi.product_name,SUM(oi.quantity) AS units,COALESCE(SUM(oi.quantity*oi.unit_price),0) AS revenue FROM order_items oi GROUP BY oi.product_id,oi.product_name ORDER BY units DESC LIMIT 10");products=cursor.fetchall()
        cursor.execute("SELECT AVG(total) AS average_order_value FROM orders");aov=cursor.fetchone()["average_order_value"];return jsonify({"daily":daily,"best_selling_products":products,"average_order_value":aov})
    finally:connection.close()


@app.get("/api/admin/business-health")
@require_admin
def admin_business_health(payload):
    """Operational health metrics calculated from persisted orders and catalogue data."""
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            """SELECT COUNT(*) AS orders,
                      COALESCE(SUM(total),0) AS revenue,
                      COALESCE(AVG(total),0) AS average_order_value,
                      COALESCE(SUM(status='delivered'),0) AS delivered,
                      COALESCE(SUM(status IN ('cancelled','returned','refunded')),0) AS lost_orders,
                      COUNT(DISTINCT user_id) AS purchasing_customers,
                      COALESCE(SUM(status IN ('placed','confirmed','processing','packed',
                                               'out_for_delivery')),0) AS pending_orders
               FROM orders WHERE created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)"""
        )
        metrics = cursor.fetchone()
        # Sales are net of terminal failed/cancelled states and are exposed for
        # both the current day and calendar month for operational decisions.
        cursor.execute(
            """SELECT
                 COALESCE(SUM(CASE WHEN DATE(created_at)=CURDATE()
                   AND status NOT IN ('cancelled','returned','refunded') THEN total ELSE 0 END),0) AS sales_today,
                 COALESCE(SUM(CASE WHEN created_at >= DATE_FORMAT(CURDATE(),'%Y-%m-01')
                   AND status NOT IN ('cancelled','returned','refunded') THEN total ELSE 0 END),0) AS sales_month
               FROM orders"""
        )
        sales = cursor.fetchone()
        metrics.update(sales)
        metrics["today_sales"] = sales["sales_today"]
        metrics["month_sales"] = sales["sales_month"]
        cursor.execute(
            """SELECT COUNT(*) AS pending_upi_payments
               FROM orders WHERE payment_method='UPI'
                 AND payment_status IN ('PENDING','PAYMENT_VERIFICATION_PENDING')"""
        )
        metrics["pending_upi_payments"] = cursor.fetchone()["pending_upi_payments"]
        cursor.execute(
            """SELECT COUNT(*) AS new_customers
               FROM users WHERE role='CUSTOMER'
                 AND created_at >= DATE_FORMAT(CURDATE(),'%Y-%m-01')"""
        )
        metrics["new_customers"] = cursor.fetchone()["new_customers"]
        cursor.execute(
            """SELECT COUNT(*) AS repeat_customers FROM (
                 SELECT user_id FROM orders
                 WHERE user_id IS NOT NULL
                   AND created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                 GROUP BY user_id HAVING COUNT(*) > 1
               ) repeats"""
        )
        metrics["repeat_customers"] = cursor.fetchone()["repeat_customers"]
        cursor.execute(
            """SELECT COUNT(*) AS active_products,
                      SUM(stock_quantity=0) AS out_of_stock,
                      SUM(stock_quantity>0 AND stock_quantity<=low_stock_threshold) AS low_stock
               FROM products WHERE is_active=1"""
        )
        metrics.update(cursor.fetchone())
        cursor.execute(
            """SELECT COUNT(*) AS open_enquiries FROM enquiries
               WHERE status NOT IN ('resolved','closed')"""
        )
        metrics["open_enquiries"] = cursor.fetchone()["open_enquiries"]
        metrics["fulfilment_rate"] = (
            round(float(metrics["delivered"]) / int(metrics["orders"]) * 100, 2)
            if metrics["orders"] else None
        )
        metrics["repeat_customer_rate"] = (
            round(int(metrics["repeat_customers"]) / int(metrics["purchasing_customers"]) * 100, 2)
            if metrics["purchasing_customers"] else None
        )
        cursor.execute(
            """SELECT p.id, p.name, p.image_url, SUM(oi.quantity) AS units,
                      COALESCE(SUM(oi.quantity * oi.unit_price),0) AS revenue
               FROM order_items oi JOIN orders o ON o.id=oi.order_id
               LEFT JOIN products p ON p.id=oi.product_id
               WHERE o.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                 AND o.status NOT IN ('cancelled','returned','refunded')
               GROUP BY p.id, p.name, p.image_url
               ORDER BY units DESC, revenue DESC LIMIT 10"""
        )
        top_products = cursor.fetchall()
        cursor.execute(
            """SELECT p.id, p.name, p.image_url, COUNT(rv.id) AS views,
                      MAX(rv.viewed_at) AS last_viewed
               FROM recently_viewed rv JOIN products p ON p.id=rv.product_id
               WHERE rv.viewed_at >= DATE_SUB(NOW(), INTERVAL 90 DAY)
               GROUP BY p.id, p.name, p.image_url
               ORDER BY views DESC, last_viewed DESC LIMIT 10"""
        )
        most_viewed = cursor.fetchall()
        cursor.execute(
            """SELECT p.id, p.name, p.image_url, COUNT(w.id) AS wishlist_count
               FROM wishlists w JOIN products p ON p.id=w.product_id
               GROUP BY p.id, p.name, p.image_url
               ORDER BY wishlist_count DESC, p.name LIMIT 10"""
        )
        most_wishlisted = cursor.fetchall()
        return jsonify({
            "period_days": 30,
            "metrics": metrics,
            "top_products": top_products,
            "most_viewed_products": most_viewed,
            "most_wishlisted_products": most_wishlisted,
        })
    finally:
        connection.close()


@app.get("/api/admin/system-health")
@require_admin
def admin_system_health(payload):
    """Expose actionable infrastructure checks; no synthetic uptime or traffic values."""
    checks = []
    started = datetime.now(timezone.utc)
    connection = None
    try:
        connection = connect()
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        checks.append({"name": "mysql", "status": "ok", "latency_ms": round(
            (datetime.now(timezone.utc) - started).total_seconds() * 1000, 2)})
    except Exception as exc:
        checks.append({"name": "mysql", "status": "error", "message": str(exc)})
    finally:
        if connection:
            connection.close()
    checks.append({"name": "uploads", "status": "ok" if os.access(str(UPLOAD_DIR), os.W_OK) else "error",
                   "path": str(UPLOAD_DIR)})
    try:
        import shutil
        disk = shutil.disk_usage(str(BASE_DIR))
        disk_status = "ok" if disk.free > 100 * 1024 * 1024 else "warning"
        checks.append({"name": "disk", "status": disk_status, "free_bytes": disk.free,
                       "total_bytes": disk.total})
    except OSError as exc:
        checks.append({"name": "disk", "status": "error", "message": str(exc)})
    overall = "ok" if all(check["status"] == "ok" for check in checks) else "degraded"
    return jsonify({"status": overall, "checked_at": datetime.now(timezone.utc).isoformat(),
                    "checks": checks})


MAINTENANCE_ACTIONS = {
    "cleanup_expired_tokens": {
        "label": "Expired password-reset tokens",
        "table": "password_reset_tokens",
        "where": "used_at IS NOT NULL OR expires_at < UTC_TIMESTAMP()",
        "default_days": None,
    },
    "cleanup_recently_viewed": {
        "label": "Recently viewed entries",
        "table": "recently_viewed",
        "where": "viewed_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)",
        "default_days": 180,
    },
    "cleanup_read_notifications": {
        "label": "Read notifications",
        "table": "notifications",
        "where": "is_read = 1 AND created_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)",
        "default_days": 365,
    },
}


def _maintenance_days(action, requested):
    default = MAINTENANCE_ACTIONS[action]["default_days"]
    if default is None:
        return None
    try:
        days = int(requested if requested is not None else default)
    except (TypeError, ValueError):
        raise ValueError("Retention days must be a whole number.")
    if not 30 <= days <= 730:
        raise ValueError("Retention days must be between 30 and 730.")
    return days


@app.get("/api/admin/maintenance")
@require_admin
def admin_maintenance_status(payload):
    """Show bounded, read-only cleanup previews before an administrator runs them."""
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        actions = []
        for action, config in MAINTENANCE_ACTIONS.items():
            days = config["default_days"]
            where = config["where"]
            params = () if days is None else (days,)
            cursor.execute(f"SELECT COUNT(*) AS total FROM {config['table']} WHERE {where}", params)
            actions.append({
                "action": action, "label": config["label"],
                "retention_days": days, "eligible_rows": cursor.fetchone()["total"],
            })
        return jsonify({"actions": actions, "safe_defaults": True})
    finally:
        connection.close()


@app.post("/api/admin/maintenance")
@require_admin
def admin_run_maintenance(payload):
    """Run one allow-listed cleanup, or all cleanups, inside one transaction."""
    data = request.get_json(silent=True) or {}
    action = str(data.get("action") or "").strip().lower()
    if action == "run_all":
        selected = list(MAINTENANCE_ACTIONS)
    elif action in MAINTENANCE_ACTIONS:
        selected = [action]
    else:
        return jsonify({"error": "Choose an allowed maintenance action."}), 400
    dry_run = data.get("dry_run", False)
    if isinstance(dry_run, str):
        dry_run = dry_run.lower() in {"1", "true", "yes", "on"}
    retention = {}
    try:
        for name in selected:
            retention[name] = _maintenance_days(name, data.get("retention_days"))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        results = []
        for name in selected:
            config = MAINTENANCE_ACTIONS[name]
            days = retention[name]
            params = () if days is None else (days,)
            cursor.execute(f"SELECT COUNT(*) AS total FROM {config['table']} WHERE {config['where']}", params)
            eligible = int(cursor.fetchone()["total"])
            deleted = 0
            if not dry_run and eligible:
                cursor.execute(
                    f"DELETE FROM {config['table']} WHERE {config['where']}", params
                )
                deleted = cursor.rowcount
            results.append({
                "action": name, "label": config["label"],
                "retention_days": days, "eligible_rows": eligible, "deleted_rows": deleted,
            })
        if dry_run:
            connection.rollback()
        else:
            write_audit(cursor, payload, "Maintenance cleanup", "maintenance", action,
                        None, {"results": results})
            connection.commit()
        return jsonify({
            "success": True, "action": action, "dry_run": bool(dry_run),
            "results": results, "completed_at": datetime.now(timezone.utc).isoformat(),
        })
    except Error:
        connection.rollback()
        raise
    finally:
        connection.close()


@app.get("/api/admin/reports")
@require_admin
def admin_reports(payload):
    report=(request.args.get("type") or "sales").lower(); connection=connect()
    try:
        cursor=connection.cursor(dictionary=True)
        queries={"sales":"SELECT DATE(created_at) AS date,COUNT(*) AS orders,COALESCE(SUM(total),0) AS revenue FROM orders GROUP BY DATE(created_at) ORDER BY date DESC","orders":"SELECT id,CONCAT('NK',LPAD(id,8,'0')) AS order_number,status,total,payment_method,payment_status,created_at FROM orders ORDER BY created_at DESC","products":"SELECT id,name,sku,price,stock_quantity,is_active,created_at FROM products ORDER BY created_at DESC","inventory":"SELECT id,name,sku,stock_quantity,low_stock_threshold,is_active FROM products ORDER BY stock_quantity","customers":"SELECT id,name,email,phone,created_at FROM users WHERE role='CUSTOMER' ORDER BY created_at DESC","transactions":"SELECT * FROM transactions ORDER BY created_at DESC"}
        cursor.execute(queries.get(report,queries["sales"]));return jsonify({"report":report,"rows":cursor.fetchall()})
    finally:connection.close()


@app.get("/api/admin/search")
@require_admin
def admin_search(payload):
    q=(request.args.get("q") or "").strip()
    if len(q)<2:return jsonify({"results":{}})
    like=f"%{q}%";connection=connect()
    try:
        cursor=connection.cursor(dictionary=True);results={}
        cursor.execute("SELECT id,CONCAT('NK',LPAD(id,8,'0')) AS label,status FROM orders WHERE CONCAT('NK',LPAD(id,8,'0')) LIKE %s LIMIT 8",(like,));results["orders"]=cursor.fetchall()
        cursor.execute("SELECT id,name,sku FROM products WHERE name LIKE %s OR sku LIKE %s LIMIT 8",(like,like));results["products"]=cursor.fetchall()
        cursor.execute("SELECT id,name,email,phone FROM users WHERE role='CUSTOMER' AND (name LIKE %s OR email LIKE %s OR phone LIKE %s) LIMIT 8",(like,like,like));results["customers"]=cursor.fetchall()
        cursor.execute("SELECT id,name,slug FROM categories WHERE name LIKE %s OR slug LIKE %s LIMIT 8",(like,like));results["categories"]=cursor.fetchall()
        cursor.execute("SELECT transaction_id,order_id,amount,status FROM transactions WHERE transaction_id LIKE %s LIMIT 8",(like,));results["transactions"]=cursor.fetchall()
        cursor.execute("SELECT id,name,phone,status FROM enquiries WHERE name LIKE %s OR phone LIKE %s OR message LIKE %s LIMIT 8",(like,like,like));results["enquiries"]=cursor.fetchall()
        return jsonify({"results":results})
    finally:connection.close()


@app.get("/api/admin/profile")
@require_admin
def admin_profile(payload):
    connection=connect()
    try:cursor=connection.cursor(dictionary=True);cursor.execute("SELECT id,name,email,phone,role,created_at,last_login FROM users WHERE id=%s",(admin_id(payload),));return jsonify({"user":cursor.fetchone()})
    finally:connection.close()


@app.put("/api/admin/profile")
@require_admin
def update_admin_profile(payload):
    data=request.get_json(silent=True) or {};name=str(data.get("name") or "").strip();phone=str(data.get("phone") or "").strip()
    if not name:return jsonify({"error":"Name is required."}),400
    connection=connect()
    try:cursor=connection.cursor();cursor.execute("UPDATE users SET name=%s,phone=%s WHERE id=%s",(name,phone,admin_id(payload)));connection.commit();return jsonify({"success":True})
    finally:connection.close()


@app.get("/api/admin/settings")
@require_admin
def admin_settings(payload):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT setting_key, setting_value, updated_at FROM settings ORDER BY setting_key")
        return jsonify({"settings": cursor.fetchall()})
    finally:
        connection.close()


@app.put("/api/admin/settings")
@require_admin
def update_admin_settings(payload):
    values = (request.get_json(silent=True) or {}).get("settings") or {}
    if not isinstance(values, dict):
        return jsonify({"error": "Settings must be an object."}), 400
    connection = connect()
    try:
        cursor = connection.cursor()
        for key, value in values.items():
            if not re.fullmatch(r"[A-Za-z0-9_.-]{1,120}", str(key)):
                return jsonify({"error": "Invalid setting key."}), 400
            if key == "upi_id":
                value = str(value or "").strip()
                if value and not UPI_ID_PATTERN.fullmatch(value):
                    return jsonify({"error": "Enter a valid UPI ID (example@bank)."}), 400
            elif key == "upi_display_name":
                value = str(value or "").strip()
                if len(value) > 120:
                    return jsonify({"error": "UPI display name must be 120 characters or fewer."}), 400
            elif key == "upi_enabled":
                value = str(value).lower()
                if value not in {"true", "false", "1", "0", "yes", "no", "on", "off"}:
                    return jsonify({"error": "Enable UPI must be true or false."}), 400
            cursor.execute(
                "INSERT INTO settings (setting_key, setting_value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE setting_value=VALUES(setting_value)",
                (key, str(value)),
            )
        write_audit(cursor, payload, "Settings updated", "settings", "store", None, values)
        connection.commit()
        return jsonify({"success": True})
    finally:
        connection.close()


SUPPORT_MAX_MESSAGE_LENGTH = 5000


def support_message_body(data):
    body = str((data or {}).get("message") or (data or {}).get("body") or "").strip()
    if not body:
        return None, "Message is required."
    if len(body) > SUPPORT_MAX_MESSAGE_LENGTH:
        return None, f"Message must be {SUPPORT_MAX_MESSAGE_LENGTH} characters or fewer."
    return body, None


def support_conversation_detail(cursor, conversation_id, user_id=None, admin=False):
    if admin:
        where, params = "sc.id=%s", (conversation_id,)
    else:
        where, params = "sc.id=%s AND sc.customer_id=%s", (conversation_id, user_id)
    cursor.execute(
        """SELECT sc.*, u.name AS customer_name, u.email AS customer_email,
                  CONCAT('NK', LPAD(sc.order_id, 8, '0')) AS order_number,
                  (SELECT COUNT(*) FROM support_messages sm WHERE sm.conversation_id=sc.id
                   AND sm.sender_type=%s AND sm.is_read=0) AS unread_count
           FROM support_conversations sc JOIN users u ON u.id=sc.customer_id
           WHERE """ + where,
        ("CUSTOMER" if admin else "ADMIN",) + params,
    )
    conversation = cursor.fetchone()
    if not conversation:
        return None
    cursor.execute(
        """SELECT sm.id, sm.conversation_id, sm.sender_type, sm.sender_id, sm.body,
                  sm.is_read, sm.created_at, u.name AS sender_name
           FROM support_messages sm JOIN users u ON u.id=sm.sender_id
           WHERE sm.conversation_id=%s ORDER BY sm.created_at ASC, sm.id ASC""",
        (conversation_id,),
    )
    conversation["messages"] = cursor.fetchall()
    return conversation


def support_response(conversation, unread_count=None):
    """Keep conversation metadata and message collections predictable for clients."""
    conversation = dict(conversation)
    messages = conversation.pop("messages", [])
    return {
        "conversation": conversation,
        "messages": messages,
        "unread_count": int(conversation.get("unread_count", 0)
                             if unread_count is None else unread_count),
    }


def support_order_id(value):
    value = str(value or "").strip()
    if not value:
        return None
    match = re.fullmatch(r"(?:NK)?(\d+)", value, re.IGNORECASE)
    if not match:
        raise ValueError("Order number must look like NK00000001.")
    return int(match.group(1))


@app.get("/api/support/conversations")
@app.get("/api/account/support/conversations")
@require_auth
def support_customer_conversations(payload):
    user_id = user_id_from_payload(payload)
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            """SELECT sc.id, sc.subject, sc.reason, sc.order_id, sc.status,
                      sc.created_at, sc.updated_at, sc.closed_at,
                      CONCAT('NK', LPAD(sc.order_id, 8, '0')) AS order_number,
                      (SELECT COUNT(*) FROM support_messages sm
                       WHERE sm.conversation_id=sc.id AND sm.sender_type='ADMIN'
                       AND sm.is_read=0) AS unread_count
               FROM support_conversations sc WHERE sc.customer_id=%s
               ORDER BY sc.updated_at DESC, sc.id DESC""",
            (user_id,),
        )
        rows = cursor.fetchall()
        return jsonify({"conversations": rows, "unread_count": sum(int(r["unread_count"]) for r in rows)})
    finally:
        connection.close()


@app.post("/api/support/conversations")
@app.post("/api/account/support/conversations")
@require_auth
def support_create_conversation(payload):
    data = request.get_json(silent=True) or {}
    body, error = support_message_body(data)
    subject = str(data.get("subject") or "Support request").strip()
    reason = str(data.get("reason") or "").strip() or None
    if error:
        return jsonify({"error": error}), 400
    if len(subject) > 180 or (reason and len(reason) > 80):
        return jsonify({"error": "Subject or reason is too long."}), 400
    user_id = user_id_from_payload(payload)
    try:
        order_id = support_order_id(data.get("order_number") or data.get("order_id"))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        if order_id is not None:
            cursor.execute("SELECT id FROM orders WHERE id=%s AND user_id=%s", (order_id, user_id))
            if not cursor.fetchone():
                return jsonify({"error": "Order not found for this account."}), 400
        cursor.execute(
            "INSERT INTO support_conversations (customer_id, subject, reason, order_id) VALUES (%s,%s,%s,%s)",
            (user_id, subject, reason, order_id),
        )
        conversation_id = cursor.lastrowid
        cursor.execute(
            "INSERT INTO support_messages (conversation_id,sender_type,sender_id,body) VALUES (%s,'CUSTOMER',%s,%s)",
            (conversation_id, user_id, body),
        )
        notify_admins(cursor, "New support conversation", subject, "SUPPORT", order_id)
        connection.commit()
        return jsonify(support_response(
            support_conversation_detail(cursor, conversation_id, user_id=user_id)
        )), 201
    finally:
        connection.close()


@app.get("/api/support/conversations/<int:conversation_id>")
@app.get("/api/account/support/conversations/<int:conversation_id>")
@require_auth
def support_customer_conversation(payload, conversation_id):
    user_id = user_id_from_payload(payload)
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        conversation = support_conversation_detail(cursor, conversation_id, user_id=user_id)
        if not conversation:
            return jsonify({"error": "Conversation not found."}), 404
        cursor.execute(
            "UPDATE support_messages SET is_read=1 WHERE conversation_id=%s AND sender_type='ADMIN'",
            (conversation_id,),
        )
        connection.commit()
        return jsonify(support_response(conversation, unread_count=0))
    finally:
        connection.close()


@app.post("/api/support/conversations/<int:conversation_id>/messages")
@app.post("/api/account/support/conversations/<int:conversation_id>/messages")
@require_auth
def support_customer_reply(payload, conversation_id):
    body, error = support_message_body(request.get_json(silent=True) or {})
    if error:
        return jsonify({"error": error}), 400
    user_id = user_id_from_payload(payload)
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM support_conversations WHERE id=%s AND customer_id=%s", (conversation_id, user_id))
        conversation = cursor.fetchone()
        if not conversation:
            return jsonify({"error": "Conversation not found."}), 404
        # Closing archives the thread, but a new customer message reopens it.
        if conversation["status"] == "CLOSED":
            cursor.execute(
                "UPDATE support_conversations SET status='OPEN', closed_at=NULL WHERE id=%s",
                (conversation_id,),
            )
        cursor.execute(
            "INSERT INTO support_messages (conversation_id,sender_type,sender_id,body) VALUES (%s,'CUSTOMER',%s,%s)",
            (conversation_id, user_id, body),
        )
        message_id = cursor.lastrowid
        notify_admins(cursor, "New support message", conversation["subject"], "SUPPORT", conversation.get("order_id"))
        connection.commit()
        detail = support_conversation_detail(cursor, conversation_id, user_id=user_id)
        message = detail["messages"][-1]
        socketio.emit(
            "support_message",
            {"conversation_id": conversation_id, "message": message},
            to=f"support:{conversation_id}",
        )
        socketio.emit(
            "support_message",
            {"conversation_id": conversation_id, "message": message},
            to="admin:support",
        )
        return jsonify(support_response(detail, unread_count=0) | {
            "success": True, "message_id": message_id,
        }), 201
    finally:
        connection.close()


@app.post("/api/support/conversations/<int:conversation_id>/close")
@app.post("/api/account/support/conversations/<int:conversation_id>/close")
@require_auth
def support_customer_close(payload, conversation_id):
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE support_conversations SET status='CLOSED', closed_at=UTC_TIMESTAMP() "
            "WHERE id=%s AND customer_id=%s AND status<>'CLOSED'",
            (conversation_id, user_id_from_payload(payload)),
        )
        if cursor.rowcount == 0:
            return jsonify({"error": "Conversation not found or already closed."}), 404
        connection.commit()
        cursor = connection.cursor(dictionary=True)
        return jsonify(support_response(
            support_conversation_detail(cursor, conversation_id, user_id=user_id_from_payload(payload)),
            unread_count=0,
        ) | {"success": True, "status": "CLOSED"})
    finally:
        connection.close()


@app.get("/api/admin/support/conversations")
@require_admin
def admin_support_conversations(payload):
    status = str(request.args.get("status") or "").upper()
    if status and status not in {"OPEN", "CLOSED"}:
        return jsonify({"error": "Status must be OPEN or CLOSED."}), 400
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        clause = "WHERE 1=1"
        params = []
        if status:
            clause += " AND sc.status=%s"
            params.append(status)
        cursor.execute(
            """SELECT sc.id, sc.customer_id, sc.subject, sc.reason, sc.order_id, sc.status,
                      sc.created_at, sc.updated_at, sc.closed_at, u.name AS customer_name,
                      u.email AS customer_email, u.phone AS customer_phone,
                      CONCAT('NK', LPAD(sc.order_id, 8, '0')) AS order_number,
                      (SELECT sm.body FROM support_messages sm
                       WHERE sm.conversation_id=sc.id
                       ORDER BY sm.created_at DESC, sm.id DESC LIMIT 1) AS last_message,
                      (SELECT COUNT(*) FROM support_messages sm WHERE sm.conversation_id=sc.id
                       AND sm.sender_type='CUSTOMER' AND sm.is_read=0) AS unread_count
               FROM support_conversations sc JOIN users u ON u.id=sc.customer_id """ + clause +
            " ORDER BY sc.updated_at DESC, sc.id DESC",
            tuple(params),
        )
        rows = cursor.fetchall()
        return jsonify({"conversations": rows, "unread_count": sum(int(r["unread_count"]) for r in rows)})
    finally:
        connection.close()


@app.get("/api/admin/support/conversations/<int:conversation_id>")
@require_admin
def admin_support_conversation(payload, conversation_id):
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        conversation = support_conversation_detail(cursor, conversation_id, admin=True)
        if not conversation:
            return jsonify({"error": "Conversation not found."}), 404
        cursor.execute(
            "UPDATE support_messages SET is_read=1 WHERE conversation_id=%s AND sender_type='CUSTOMER'",
            (conversation_id,),
        )
        connection.commit()
        return jsonify(support_response(conversation, unread_count=0))
    finally:
        connection.close()


@app.post("/api/admin/support/conversations/<int:conversation_id>/messages")
@require_admin
def admin_support_reply(payload, conversation_id):
    body, error = support_message_body(request.get_json(silent=True) or {})
    if error:
        return jsonify({"error": error}), 400
    admin_user_id = admin_id(payload)
    connection = connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM support_conversations WHERE id=%s", (conversation_id,))
        conversation = cursor.fetchone()
        if not conversation:
            return jsonify({"error": "Conversation not found."}), 404
        # Closing archives the thread, but a new admin reply reopens it.
        if conversation["status"] == "CLOSED":
            cursor.execute(
                "UPDATE support_conversations SET status='OPEN', closed_at=NULL WHERE id=%s",
                (conversation_id,),
            )
        cursor.execute(
            "INSERT INTO support_messages (conversation_id,sender_type,sender_id,body) VALUES (%s,'ADMIN',%s,%s)",
            (conversation_id, admin_user_id, body),
        )
        message_id = cursor.lastrowid
        create_notification(
            cursor, conversation["customer_id"], "Support reply", conversation["subject"],
            "SUPPORT", conversation.get("order_id"),
        )
        connection.commit()
        detail = support_conversation_detail(cursor, conversation_id, admin=True)
        message = detail["messages"][-1]
        socketio.emit(
            "support_message",
            {"conversation_id": conversation_id, "message": message},
            to=f"support:{conversation_id}",
        )
        return jsonify(support_response(detail, unread_count=0) | {
            "success": True, "message_id": message_id,
        }), 201
    finally:
        connection.close()


@app.post("/api/admin/support/conversations/<int:conversation_id>/close")
@require_admin
def admin_support_close(payload, conversation_id):
    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE support_conversations SET status='CLOSED', closed_at=UTC_TIMESTAMP() "
            "WHERE id=%s AND status<>'CLOSED'", (conversation_id,),
        )
        if cursor.rowcount == 0:
            return jsonify({"error": "Conversation not found or already closed."}), 404
        connection.commit()
        cursor = connection.cursor(dictionary=True)
        return jsonify(support_response(
            support_conversation_detail(cursor, conversation_id, admin=True),
            unread_count=0,
        ) | {"success": True, "status": "CLOSED"})
    finally:
        connection.close()


@app.post("/api/enquiries")
def create_enquiry():
    data = request.get_json(silent=True) or {}
    required = ("name", "phone", "message")
    missing = [field for field in required if not data.get(field)]
    if missing:
        return jsonify({"error": f"Missing fields: {', '.join(missing)}"}), 400

    connection = connect()
    try:
        cursor = connection.cursor()
        cursor.execute(
            "INSERT INTO enquiries (name, email, phone, message) VALUES (%s, %s, %s, %s)",
            (data["name"], data.get("email"), data["phone"], data["message"]),
        )
        connection.commit()
        return jsonify({"id": cursor.lastrowid, "status": "received"}), 201
    finally:
        connection.close()


if __name__ == "__main__":
    import sys

    if "--check-db" in sys.argv:
        initialize_database()
        print("MySQL connection and nakoda_db schema are ready.")
    else:
        initialize_database()
        socketio.run(
            app,
            host="0.0.0.0",
            port=int(os.getenv("PORT", "5000")),
            debug=False,
            allow_unsafe_werkzeug=True,
        )
