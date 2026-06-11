PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

-- Owned by marina_management
CREATE TABLE IF NOT EXISTS marinas (
    id INT,
    name TEXT,
    city TEXT,
    state TEXT,
    lat REAL,
    lon REAL,
    vhf_channel TEXT,
    website TEXT,
    phone TEXT,
    fuel_json NUM,
    amenities_json NUM,
    tech_json NUM,
    proximity_json NUM,
    last_updated NUM,
    src_url TEXT,
    marina_uid TEXT,
    website_url TEXT,
    marinas_url TEXT,
    dockwa_url TEXT,
    source_marinas_id TEXT,
    dockwa_destination_id TEXT,
    aliases_json TEXT,
    verification_state TEXT,
    missing_from_web_count INTEGER,
    fuel_candidate INTEGER,
    seed_reason TEXT,
    last_seen_on_web_utc TEXT,
    features_last_checked_at_utc TEXT,
    last_fuel_checked_at_utc TEXT,
    sync_dirty INTEGER,
    created_at_utc TEXT,
    updated_at_utc TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_marinas_marina_uid
    ON marinas(marina_uid)
    WHERE marina_uid IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_marinas_source_marinas_id
    ON marinas(source_marinas_id)
    WHERE source_marinas_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_marinas_dockwa_destination_id
    ON marinas(dockwa_destination_id)
    WHERE dockwa_destination_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_marinas_geo
    ON marinas(lat, lon);

CREATE INDEX IF NOT EXISTS idx_marinas_fuel_candidate
    ON marinas(fuel_candidate, sync_dirty);

-- Published by marina_management, consumed by fuel_extractor
CREATE TABLE IF NOT EXISTS fuel_seed_queue (
    seed_id INTEGER PRIMARY KEY,
    marina_uid TEXT NOT NULL,
    name TEXT NOT NULL,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    website_url TEXT,
    marinas_url TEXT,
    dockwa_url TEXT,
    fuel_candidate INTEGER NOT NULL CHECK (fuel_candidate IN (0, 1)),
    seed_reason TEXT NOT NULL,
    seeded_at_utc TEXT NOT NULL,
    source_marinas_id TEXT,
    dockwa_destination_id TEXT,
    last_fuel_checked_at_utc TEXT,
    priority_hint TEXT,
    queue_status TEXT NOT NULL CHECK (queue_status IN ('pending', 'processing', 'done', 'failed')),
    FOREIGN KEY (marina_uid) REFERENCES marinas(marina_uid) ON DELETE CASCADE,
    CHECK (
        dockwa_url IS NOT NULL
        OR marinas_url IS NOT NULL
        OR website_url IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS idx_fuel_seed_queue_status
    ON fuel_seed_queue(queue_status, seeded_at_utc);

-- Owned by fuel_extractor
CREATE TABLE IF NOT EXISTS fuel_logs (
    fuel_log_id INTEGER PRIMARY KEY,
    marina_uid TEXT NOT NULL,
    fetched_at_utc TEXT NOT NULL,
    outcome_state TEXT NOT NULL CHECK (outcome_state IN ('has_public_price', 'fuel_available_price_hidden', 'fuel_unknown', 'fetch_blocked')),
    reason_tag TEXT NOT NULL,
    blocked_reason TEXT CHECK (
        blocked_reason IS NULL
        OR blocked_reason IN (
            'access_denied_401',
            'access_denied_403',
            'rate_limited_429',
            'cloudflare_challenge',
            'dns_failure',
            'ssl_failure',
            'timeout'
        )
    ),
    diesel_price REAL,
    gasoline_price REAL,
    fuel_dock INTEGER CHECK (fuel_dock IS NULL OR fuel_dock IN (0, 1)),
    last_updated TEXT,
    source_url TEXT,
    source_text TEXT,
    provenance_json TEXT NOT NULL,
    price_source TEXT NOT NULL CHECK (price_source IN ('dockwa_json', 'marinas_web', 'website_text', 'not_published_online', 'none')),
    confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    extraction_hash TEXT,
    sync_dirty INTEGER NOT NULL DEFAULT 0,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY (marina_uid) REFERENCES marinas(marina_uid) ON DELETE CASCADE,
    CHECK (json_valid(provenance_json)),
    CHECK (
        outcome_state != 'has_public_price'
        OR diesel_price IS NOT NULL
        OR gasoline_price IS NOT NULL
    ),
    CHECK (
        outcome_state != 'fuel_available_price_hidden'
        OR fuel_dock = 1
    ),
    CHECK (
        outcome_state != 'fetch_blocked'
        OR blocked_reason IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS idx_fuel_logs_marina_time
    ON fuel_logs(marina_uid, fetched_at_utc);

CREATE INDEX IF NOT EXISTS idx_fuel_logs_outcome
    ON fuel_logs(outcome_state, reason_tag);

-- Sync/audit bridge between marina_management and fuel_extractor
CREATE TABLE IF NOT EXISTS sync_events (
    sync_event_id INTEGER PRIMARY KEY,
    marina_uid TEXT NOT NULL,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('marina', 'fuel_log', 'pricing_log', 'extraction')),
    entity_ref TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN (
            'new_discovery',
            'rebrand_detected',
            'marked_unverified',
            'dockwa_link_added',
            'fuel_price_changed',
            'fetch_blocked',
            'data_synced'
        )
    ),
    reason_tag TEXT NOT NULL,
    before_hash TEXT,
    after_hash TEXT,
    sync_dirty_before INTEGER NOT NULL CHECK (sync_dirty_before IN (0, 1)),
    sync_dirty_after INTEGER NOT NULL CHECK (sync_dirty_after IN (0, 1)),
    master_status_code INTEGER,
    master_acknowledged INTEGER NOT NULL CHECK (master_acknowledged IN (0, 1)),
    occurred_at_utc TEXT NOT NULL,
    processed_at_utc TEXT,
    fetch_method TEXT,
    FOREIGN KEY (marina_uid) REFERENCES marinas(marina_uid) ON DELETE CASCADE,
    CHECK (
        master_acknowledged = 0
        OR master_status_code = 200
    )
);

CREATE INDEX IF NOT EXISTS idx_sync_events_pending
    ON sync_events(master_acknowledged, occurred_at_utc);

CREATE INDEX IF NOT EXISTS idx_sync_events_fetch_method
    ON sync_events(fetch_method, entity_type);

-- Marina discovery state tracking
CREATE TABLE IF NOT EXISTS discovery_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_discovery_lat REAL,
    last_discovery_lon REAL,
    last_discovery_time TEXT,
    discovery_count INTEGER NOT NULL DEFAULT 0,
    discovery_threshold_miles REAL NOT NULL DEFAULT 10,
    min_discovery_interval_hours REAL NOT NULL DEFAULT 1
);

-- Pricing logs for slip rates and amenities
CREATE TABLE IF NOT EXISTS pricing_logs (
    pricing_log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    marina_uid TEXT NOT NULL,
    fetched_at_utc TEXT NOT NULL,
    monthly_base REAL,
    is_per_ft INTEGER CHECK (is_per_ft IS NULL OR is_per_ft IN (0, 1)),
    catamaran_multiplier REAL,
    liveaboard_fee REAL,
    min_air_draft_ft REAL,
    air_draft_source TEXT,
    min_depth_ft REAL,
    depth_source TEXT,
    lift_max_beam_ft REAL,
    lift_max_tons REAL,
    diy_allowed INTEGER CHECK (diy_allowed IS NULL OR diy_allowed IN (0, 1)),
    electricity_metered INTEGER CHECK (electricity_metered IS NULL OR electricity_metered IN (0, 1)),
    water_metered INTEGER CHECK (water_metered IS NULL OR water_metered IN (0, 1)),
    liveaboard_permitted INTEGER CHECK (liveaboard_permitted IS NULL OR liveaboard_permitted IN (0, 1)),
    source_quotes TEXT,
    extraction_hash TEXT,
    sync_dirty INTEGER NOT NULL CHECK (sync_dirty IN (0, 1)),
    created_at_utc TEXT NOT NULL,
    CHECK (json_valid(source_quotes))
);

CREATE INDEX IF NOT EXISTS idx_pricing_logs_marina_time
    ON pricing_logs(marina_uid, fetched_at_utc);

CREATE INDEX IF NOT EXISTS idx_pricing_logs_sync_dirty
    ON pricing_logs(sync_dirty);

-- Bridges (GIS/spatial data)
CREATE TABLE IF NOT EXISTS bridges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT,
    name TEXT NOT NULL,
    state TEXT,
    city TEXT,
    latitude REAL,
    longitude REAL,
    closed_height_mhw TEXT,
    tier TEXT,
    schedule_type TEXT,
    opening_intervals TEXT,
    blackout_windows TEXT,
    vhf_channel TEXT,
    source_url TEXT UNIQUE,
    raw_data TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    "geometry" POINT,
    tier_description TEXT,
    phone TEXT,
    normally_open_closed TEXT,
    has_seasonal_variation BOOLEAN,
    current_rule_summary TEXT,
    seasonal_data JSON,
    constraints JSON,
    last_agent_audit DATETIME,
    last_phone_audit DATETIME,
    agent_notes TEXT,
    bridge_type TEXT,
    last_updated DATETIME
);

CREATE INDEX IF NOT EXISTS idx_bridges_state ON bridges(state);
CREATE INDEX IF NOT EXISTS idx_bridges_city ON bridges(city);
CREATE INDEX IF NOT EXISTS idx_bridges_source_url ON bridges(source_url);
CREATE INDEX IF NOT EXISTS idx_bridges_coords ON bridges(latitude, longitude);

-- Anchorages
CREATE TABLE IF NOT EXISTS anchorages (
    id INT,
    name TEXT,
    city TEXT,
    state TEXT,
    lat REAL,
    lon REAL,
    source_url TEXT,
    raw_data_json TEXT,
    last_updated NUM,
    location TEXT,
    mile_marker TEXT,
    lat_lon_text TEXT,
    depth TEXT,
    description TEXT,
    holding_rating REAL,
    wind_protection_rating REAL,
    current_flow_rating REAL,
    wake_protection_rating REAL,
    scenic_beauty_rating REAL,
    ease_of_shopping_rating REAL,
    shore_access_rating REAL,
    pet_friendly_rating REAL,
    cell_service_rating REAL,
    wifi_rating REAL
);

-- NOAA tide/current stations
CREATE TABLE IF NOT EXISTS noaa_stations (
    id TEXT PRIMARY KEY,
    name TEXT,
    state TEXT,
    data_type TEXT,
    station_type TEXT,
    lat REAL,
    lng REAL,
    raw_metadata TEXT,
    "geometry" POINT
);

-- Fuel sources (manually entered / third party)
CREATE TABLE IF NOT EXISTS fuel_sources (
    id INTEGER PRIMARY KEY,
    external_ref TEXT,
    source_type TEXT NOT NULL CHECK (source_type IN ('marina', 'fuel_dock', 'gas_station', 'other')),
    name TEXT NOT NULL,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    website_url TEXT,
    dockside_access INTEGER CHECK (dockside_access IN (0, 1) OR dockside_access IS NULL),
    vessel_access_notes TEXT,
    diesel_available INTEGER CHECK (diesel_available IN (0, 1) OR diesel_available IS NULL),
    diesel_price REAL,
    gasoline_available INTEGER CHECK (gasoline_available IN (0, 1) OR gasoline_available IS NULL),
    gasoline_price REAL,
    non_ethanol_available INTEGER CHECK (non_ethanol_available IN (0, 1) OR non_ethanol_available IS NULL),
    valvtect_available INTEGER CHECK (valvtect_available IN (0, 1) OR valvtect_available IS NULL),
    price_currency TEXT,
    price_unit TEXT,
    last_verified_at TEXT,
    data_source TEXT,
    confidence REAL,
    raw_extract_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    phone TEXT
);

-- Data licenses
CREATE TABLE IF NOT EXISTS data_licenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    url TEXT
);

-- SQL audit log
CREATE TABLE IF NOT EXISTS sql_statements_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time_start TIMESTAMP NOT NULL DEFAULT '0000-01-01T00:00:00.000Z',
    time_end TIMESTAMP NOT NULL DEFAULT '0000-01-01T00:00:00.000Z',
    user_agent TEXT NOT NULL,
    sql_statement TEXT NOT NULL,
    success INTEGER NOT NULL DEFAULT 0,
    error_cause TEXT NOT NULL DEFAULT 'ABORTED',
    CONSTRAINT sqllog_success CHECK (success IN (0,1))
);

COMMIT;
