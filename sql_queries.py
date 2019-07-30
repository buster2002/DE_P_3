import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES


user_table_create = ("""CREATE TABLE users(
                                            user_id              INTEGER,
                                            user_first_name      TEXT        NULL,
                                            user_last_name       TEXT        NULL,
                                            user_gender          TEXT        NULL,
                                            user_level           TEXT        NULL,
                                            PRIMARY KEY(user_id))
                    """)

song_table_create = ("""CREATE TABLE songs(
                                            song_id              TEXT,
                                            song_title           TEXT        NULL,
                                            artist_id            TEXT        NOT NULL,
                                            song_year            INTEGER     NULL,
                                            song_duration        FLOAT       NULL,
                                            PRIMARY KEY(song_id))
                    """)

artist_table_create = ("""CREATE TABLE artists(
                                                artist_id   TEXT,
                                                artist_name           TEXT        NULL,
                                                artist_location       TEXT        NULL,
                                                artist_latitude       FLOAT       NULL,
                                                artist_longitude      FLOAT       NULL,
                                                PRIMARY KEY(artist_id))
                        """)

time_table_create = ("""CREATE TABLE time(
                                            start_time      TIMESTAMP,
                                            hour            INTEGER        NULL,
                                            day             INTEGER        NULL,
                                            week            INTEGER        NULL,
                                            month           INTEGER        NULL,
                                            year            INTEGER        NULL,
                                            weekday         INTEGER        NULL,
                                            PRIMARY KEY(start_time))
                    """)
staging_events_table_create= ("""CREATE TABLE staging_events(
                                                            event_id        INTEGER IDENTITY(0,1) NOT NULL,
                                                            artist_name     TEXT                  NULL,
                                                            auth            TEXT                  NULL,
                                                            user_first_name TEXT                  NULL,
                                                            user_gender     TEXT                  NULL,
                                                            item_in_session INTEGER               NULL,
                                                            user_last_name  TEXT                  NULL,
                                                            length          FLOAT                 NULL,
                                                            user_level      TEXT                  NULL,
                                                            artist_location TEXT                  NULL,
                                                            method          TEXT                  NULL,
                                                            page            TEXT                  NULL,
                                                            registration    TEXT                  NULL,
                                                            session_id      INTEGER               NULL,
                                                            song_title      TEXT                  NULL,
                                                            status          INTEGER               NULL,
                                                            ts              TEXT                  NULL,
                                                            user_agent      TEXT                  NULL,
                                                            user_id         INTEGER               NULL,
                                                            PRIMARY KEY(event_id))
                                """)

staging_songs_table_create = ("""CREATE TABLE staging_songs(
                                                            num_songs           INTEGER     NULL,
                                                            artist_id           TEXT        NULL,
                                                            artist_latitude     FLOAT       NULL,
                                                            artist_longitude    FLOAT       NULL,
                                                            artist_location     TEXT        NULL,
                                                            artist_name         TEXT        NULL,
                                                            song_id             TEXT        NOT NULL,
                                                            title               TEXT        NULL,
                                                            duration            FLOAT       NULL,
                                                            year                INTEGER     NULL,
                                                            PRIMARY KEY(song_id))
                                """)

songplay_table_create = ("""CREATE TABLE songplays(
                                                    songplay_id        INTEGER     IDENTITY(0,1),
                                                    start_time         TIMESTAMP                  NOT NULL,
                                                    user_id            INTEGER                    NOT NULL,
                                                    user_level         TEXT                       NULL,
                                                    song_id            TEXT                       NOT NULL,
                                                    artist_id          TEXT                       NOT NULL,
                                                    session_id         INTEGER                    NULL,
                                                    artist_location    TEXT                       NULL,
                                                    user_agent         TEXT                       NULL,
                                                    PRIMARY KEY(songplay_id))
                            """)

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                        iam_role {}
                        region 'us-west-2'
                        format as json {}
                        STATUPDATE ON ;
                        """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))
staging_songs_copy = ("""COPY staging_songs FROM {}
                        iam_role {}
                        region 'us-west-2'
                        JSON 'auto' ;
                        """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, user_level, song_id, artist_id, session_id, artist_location, user_agent)
                            SELECT DISTINCT
                                TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
                                se.user_id,
                                se.user_level,
                                ss.song_id,
                                ss.artist_id,
                                se.session_id,
                                se.artist_location,
                                se.user_agent
                            FROM
                                staging_events se, staging_songs ss
                            WHERE
                                se.page = 'NextSong'
                                AND ss.title = se.song_title
                                AND ss.artist_name = se.artist_name
                                AND user_id NOT IN (SELECT DISTINCT sp.user_id
                                                    FROM
                                                        songplays sp
                                                    WHERE
                                                        sp.user_id = user_id
                                                        AND sp.start_time = start_time
                                                        AND sp.session_id = session_id )
                        """)

user_table_insert = ("""INSERT INTO users (user_id, user_first_name, user_last_name, user_gender, user_level)
                        SELECT DISTINCT
                            user_id,
                            user_first_name,
                            user_last_name,
                            user_gender,
                            user_level
                        FROM
                            staging_events
                        WHERE
                            page = 'NextSong'
                            AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
                    """)

song_table_insert = ("""INSERT INTO songs (song_id, song_title, artist_id, song_year, song_duration)
                        SELECT DISTINCT
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration
                        FROM
                            staging_songs
                        WHERE
                            song_id NOT IN (SELECT DISTINCT song_id FROM songs)
                    """)

artist_table_insert = ("""INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                            SELECT DISTINCT
                                artist_id,
                                artist_name,
                                artist_location,
                                artist_longitude,
                                artist_latitude
                            FROM
                                staging_songs
                            WHERE
                                artist_id NOT IN (SELECT artist_id from artists)
                        """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT
                            start_time,
                            EXTRACT(hr from start_time) AS hour,
                            EXTRACT(d from start_time) AS day,
                            EXTRACT(w from start_time) AS week,
                            EXTRACT(mon from start_time) AS month,
                            EXTRACT(yr from start_time) AS year,
                            EXTRACT(weekday from start_time) AS weekday
                        FROM (
                                SELECT DISTINCT
                                    TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time
                                FROM staging_events se
                            )
                        WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
                    """)


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create]

drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, staging_events_table_drop, 
                      staging_songs_table_drop, songplay_table_drop]

copy_table_queries = [staging_songs_copy, staging_events_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

