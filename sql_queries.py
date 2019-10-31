import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS sparkify_user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS start_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events 
(
    artist          VARCHAR(300),
    auth            VARCHAR(25),
    first_name      VARCHAR(25),
    gender          VARCHAR(1),
    item_in_session INTEGER, 
    last_name       VARCHAR(25),
    legnth          DECIMAL(9, 5),
    level           VARCHAR(10),
    location        VARCHAR(300),
    method          VARCHAR(6),
    page            VARCHAR(50),
    registration    DECIMAL(14, 1),
    session_id      INTEGER,
    song            VARCHAR(300),
    status          INTEGER,
    ts              BIGINT,
    user_agent      VARCHAR(150),
    user_id         VARCHAR(10)
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
    num_songs        INTEGER,
    artist_id        VARCHAR(25), 
    artist_latitude  DECIMAL(10, 5),
    artist_longitude DECIMAL(10, 5),
    artist_location  VARCHAR(300),
    artist_name      VARCHAR(300),
    song_id          VARCHAR(25),
    title            VARCHAR(300),
    duration         DECIMAL(9, 5),
    year             INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE songplay 
(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL, 
    user_id     VARCHAR(10),
    level       VARCHAR(10),
    song_id     VARCHAR(300) NOT NULL,
    artist_id   VARCHAR(25) NOT NULL,
    session_id  INTEGER,
    location    VARCHAR(300),
    user_agent  VARCHAR(150)
)
""")

user_table_create = ("""
CREATE TABLE sparkify_user 
(
    user_id    VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(25),
    last_name  VARCHAR(25),
    gender     VARCHAR(1),
    level      VARCHAR(10)
)
""")

song_table_create = ("""
CREATE TABLE song 
(
    song_id   VARCHAR(25) PRIMARY KEY,
    title     VARCHAR(300) NOT NULL,
    artist_id VARCHAR(25),
    year      INTEGER,
    duration  DECIMAL(9, 5) NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE artist 
(
    artist_id VARCHAR(25) PRIMARY KEY,
    name      VARCHAR(300) NOT NULL,
    location  VARCHAR(300),
    lattitude DECIMAL(10, 5),
    longitude DECIMAL(10, 5)
)
""")

time_table_create = ("""
CREATE TABLE start_time 
(
    start_time TIMESTAMP PRIMARY KEY,
    hour       INTEGER,
    day        INTEGER,
    week       INTEGER,
    month      INTEGER,
    year       INTEGER,
    weekday    INTEGER
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
iam_role {}
FORMAT AS JSON {};
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'), 
    config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from {} 
iam_role {}
FORMAT AS JSON 'auto';
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
insert into songplay(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
) 
select 
    timestamp 'epoch' + (se.ts / 1000) * interval '1 second',
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
from staging_events se
inner join staging_songs ss on se.song = ss.title and se.artist = ss.artist_name and se.legnth = ss.duration
where se.page = 'NextSong'
""")

user_table_insert = ("""
insert into sparkify_user(
    user_id,
    first_name,
    last_name,
    gender,
    level
)
select
    se.user_id,
    se.first_name,
    se.last_name,
    se.gender,
    se.level
from staging_events se
where not exists (select 1 from staging_events se2 where se.user_id = se2.user_id and se.ts < se2.ts)
""")

song_table_insert = ("""
insert into song(
    song_id,
    title,
    artist_id,
    year,
    duration
)
select
    ss.song_id,
    ss.title,
    ss.artist_id,
    case when ss.year != 0 then ss.year else null end as year,
    ss.duration
from staging_songs ss
""")

artist_table_insert = ("""
insert into artist(
    artist_id,
    name,
    location,
    lattitude,
    longitude
)
select 
    artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from (
  select
      ss.artist_id,
      ss.artist_name,
      ss.artist_location,
      ss.artist_latitude,
      ss.artist_longitude,
      row_number() over(partition by ss.artist_id order by ss.year desc)
  from staging_songs ss
)
where row_number = 1
""")

time_table_insert = ("""
insert into start_time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
select 
    start_time,
    extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year,
    extract(dow from start_time) as weekday
from (
  select 
  distinct sp.start_time
  from songplay sp
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
