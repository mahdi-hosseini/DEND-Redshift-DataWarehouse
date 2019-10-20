from pipeline import read_config

# CONFIG
config = read_config("config/dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR,
    length DOUBLE PRECISION,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId BIGINT,
    song VARCHAR,
    status INT,
    ts DOUBLE PRECISION,
    userAgent VARCHAR,
    userId BIGINT
)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_location VARCHAR,
    song_id VARCHAR,
    num_songs INT,
    title VARCHAR,
    duration DOUBLE PRECISION,
    artist_latitude DOUBLE PRECISION,
    artist_name VARCHAR,
    year INT,
    artist_id VARCHAR,
    artist_longitude DOUBLE PRECISION
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1) SORTKEY DISTKEY,
    start_time TIMESTAMP,
    user_id BIGINT NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id BIGINT,
    location VARCHAR,
    user_agent VARCHAR,
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY SORTKEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender CHAR(1),
    level VARCHAR,
    CONSTRAINT users_name_level_uniqe UNIQUE (user_id, level)
) DISTSTYLE ALL;
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY SORTKEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration DOUBLE PRECISION
) DISTSTYLE ALL;
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY SORTKEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
) DISTSTYLE ALL;
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
) DISTSTYLE ALL;
"""

# STAGING TABLES

staging_events_copy = (
    """
COPY staging_events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
JSON '{}'
MANIFEST
COMPUPDATE OFF STATUPDATE OFF;
"""
).format(
    config["S3"]["LOG_DATA_MANIFEST"],
    config["IAM_ROLE"]["ARN"],
    config["S3"]["LOG_JSONPATH"],
)

staging_songs_copy = (
    """
COPY staging_songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
JSON 'auto'
MANIFEST
COMPUPDATE OFF STATUPDATE OFF;
"""
).format(config["S3"]["SONG_DATA_MANIFEST"], config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = [
    """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    WITH songs_artist AS (
      SELECT
        s.song_id,
        s.artist_id,
        s.duration,
        s.title,
        a.name
    FROM
        songs s JOIN artists a ON s.artist_id = a.artist_id
    ) SELECT
        TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time,
        userid,
        level,
        song_id,
        artist_id,
        sessionid,
        location,
        useragent
    FROM
        staging_events se
          JOIN songs_artist sa
            ON se.artist = sa.name
                 AND se.song = sa.title
                 AND se.length = sa.duration
    WHERE
        page = 'NextSong';
"""
]

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
    WITH users_ranked AS (
          SELECT
                 userid,
                 firstname,
                 lastname,
                 gender,
                 level,
                 ROW_NUMBER() OVER (PARTITION BY userid ORDER BY userid, ts DESC) AS userid_ranked
          FROM
                 staging_events
          WHERE
                 page = 'NextSong'
                   AND
                 userid NOT IN (SELECT user_id FROM users)
    ) SELECT
           userid AS user_id,
           firstname AS first_name,
           lastname AS last_name,
           gender,
           level
    FROM
           users_ranked
    WHERE
           users_ranked.userid_ranked = 1;
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
    WITH songs_ranked AS (
        SELECT
            song_id,
            title,
            artist_id,
            NULLIF(year, 0),
            duration,
            ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY artist_id, song_id DESC) AS song_id_ranked
        FROM
            staging_songs
        WHERE
            song_id IS NOT NULL
            AND artist_id IS NOT NULL
            AND song_id NOT IN (SELECT song_id FROM songs)
    )
    SELECT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        songs_ranked
    WHERE
        songs_ranked.song_id_ranked = 1;
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    WITH artists_ranked AS (
        SELECT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_id, artist_name DESC) AS artist_id_ranked
        FROM
            staging_songs
        WHERE
            artist_id IS NOT NULL
            AND
            artist_name IS NOT NULL
            AND
            artist_id NOT IN (SELECT artist_id FROM artists)
    )
    SELECT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM
        artists_ranked
    WHERE
        artists_ranked.artist_id_ranked = 1;
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    WITH timestamp_fixed AS (
        SELECT
          DISTINCT TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS ts
        FROM
          staging_events
        WHERE
          page = 'NextSong'
    ) SELECT
        ts AS start_time,
        EXTRACT(hour FROM ts) AS hour,
        EXTRACT(day FROM ts) AS day,
        EXTRACT(week FROM ts) AS week,
        EXTRACT(month FROM ts) AS month,
        EXTRACT(year FROM ts) AS year,
        EXTRACT(dow FROM ts) AS weekday
    FROM
        timestamp_fixed
    WHERE
        start_time NOT IN (SELECT start_time FROM time)
"""

# UDPATE QUERIES

user_table_update = """
WITH users_ranked (
       SELECT
              userid,
              firstname,
              lastname,
              gender,
              level,
              ROW_NUMBER() OVER (PARTITION BY userid ORDER BY userid, ts DESC) AS userid_ranked
       FROM
              staging_events
       WHERE
              page = 'NextSong'
                AND
              userid IN (SELECT user_id FROM users)
) UPDATE
       users
SET
    first_name = users_ranked.firstname,
    lastname = users_ranked.lastname,
    gender = users_ranked.gender,
    level = users_ranked.level
FROM
    users_ranked
WHERE
    users.user_id = users_ranked.user_id;
"""

song_table_update = """
WITH songs_ranked AS (
    SELECT
        song_id,
        title,
        artist_id,
        NULLIF(year, 0),
        duration,
        ROW_NUMBER() OVER (PARTITION BY song_id ORDER BY artist_id, song_id DESC) AS song_id_ranked
    FROM
        staging_songs
    WHERE
        song_id IS NOT NULL
        AND artist_id IS NOT NULL
        AND song_id IN (SELECT song_id FROM songs)
) UPDATE
       songs
SET
    song_id = songs_ranked.song_id,
    title = songs_ranked.title,
    artist_id = songs_ranked.artist_id,
    year = NULLIF(songs_ranked.year, 0),
    duration = songs_ranked.duration
FROM
    songs_ranked
WHERE
    songs.song_id = songs_ranked.song_id;
"""

artist_table_update = """
WITH artists_ranked AS (
    SELECT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_id, artist_name DESC) AS artist_id_ranked
    FROM
        staging_songs
    WHERE
        artist_id IS NOT NULL
        AND
        artist_name IS NOT NULL
        AND
        artist_id IN (SELECT artist_id FROM artists)
) UPDATE
       artists
SET
    artist_id = artists_ranked.artist_id,
    name = artists_ranked.artist_name,
    location = artists_ranked.artist_location,
    latitude = artists_ranked.artist_latitude,
    longitude = artists_ranked.longitude
FROM
    artists_ranked
WHERE
    artists.artist_id = artists_ranked.artist_id;

"""


# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
update_table_queries = [
    user_table_update,
    song_table_update,
    artist_table_update,
]
