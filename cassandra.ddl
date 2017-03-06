DROP TABLE finncars.acq_car_h;

CREATE TABLE finncars.acq_car_h (
    finnkode int,
    load_date timestamp,
    km text,
    location text,
    price text,
    title text,
    year text,
    url text,
    load_time timestamp,
    PRIMARY KEY (finnkode, load_date)
) WITH CLUSTERING ORDER BY (load_date DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy', 'max_sstable_age_days': '365', 'base_time_seconds': '3600', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';


DROP TABLE finncars.acq_car_d;

CREATE TABLE finncars.acq_car_d (
    finnkode int,
    load_date timestamp,
    deleted boolean,
    equipment text,
    information text,
    properties text,
    url text,
    load_time timestamp,
    PRIMARY KEY (finnkode, load_date)
) WITH CLUSTERING ORDER BY (load_date DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy', 'max_sstable_age_days': '365', 'base_time_seconds': '3600', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

DROP TABLE finncars.prop_car_daily;

CREATE TABLE finncars.prop_car_daily (
    finnkode int,
    load_date timestamp,
    deleted boolean,
    equipment text,
    information text,
    km int,
    location text,
    price text,
    properties text,
    sold boolean,
    title text,
    year int,
    url text,
    load_time timestamp,
    PRIMARY KEY (finnkode, load_date)
) WITH CLUSTERING ORDER BY (load_date DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy', 'max_sstable_age_days': '365', 'base_time_seconds': '3600', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';


    DROP TABLE finncars.btl_car;

    CREATE TABLE finncars.btl_car (
    finnkode int PRIMARY KEY,
    antall_eiere int,
    automatgir boolean,
    cruisekontroll boolean,
    deleted boolean,
    deleted_date text,
    drivstoff text,
    effekt int,
    farge text,
    fylke text,
    hengerfeste boolean,
    km int,
    kommune text,
    last_updated text,
    lead_time_deleted int,
    lead_time_sold int,
    load_date_first timestamp,
    load_date_latest timestamp,
    location text,
    navigasjon boolean,
    parkeringsensor boolean,
    price_delta int,
    price_first int,
    price_last int,
    regnsensor boolean,
    servicehefte boolean,
    skinninterior text,
    sold boolean,
    sold_date timestamp,
    sportsseter boolean,
    sylindervolum double,
    tilstandsrapport boolean,
    title text,
    vekt int,
    xenon boolean,
    year int,
    url text,
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy', 'max_sstable_age_days': '365', 'base_time_seconds': '3600', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';


DROP TABLE finncars.scraping_log;

CREATE TABLE finncars.scraping_log(
    finnkode int,
    load_date timestamp,
    load_time timestamp,
    PRIMARY KEY (load_date, finnkode)
) WITH CLUSTERING ORDER BY (finnkode ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy', 'max_sstable_age_days': '365', 'base_time_seconds': '3600', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

DROP TABLE finncars.last_successful_load;

CREATE TABLE finncars.last_successful_load(
    table_name text,
    load_date timestamp,
    PRIMARY KEY (table_name)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'}
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy', 'max_sstable_age_days': '365', 'base_time_seconds': '3600', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';