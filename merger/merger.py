from db.pghandler import Connection
from db.tables import *

LINKS=4.73
BOVEN=52.43
RECHTS=5.015
ONDER=52.285


def merge():
    # Drop and create tables
    dropTables()
    dropTables("_t")
    createTables()
    createTable(StopTable(), "_t")
    # Stop Table Temp
    print 'Creating temp stop table'
    sql = '''
    INSERT INTO %s_t (%s)
    SELECT DISTINCT ON (%s) %s FROM %s_raw
    ORDER BY %s, dataset_id desc
    ''' % (StopTable().name, StopTable().fieldnames,
           StopTable().pks, StopTable().fieldnames, StopTable().name, StopTable().pks)
    with Connection() as con:
        con.execute(sql)
    # StopTimes Table
    print 'Creating stop times table'
    sql = '''
    WITH st AS (SELECT %s, max(dataset_id) md FROM %s_raw WHERE %s IN
    (SELECT %s FROM %s_t WHERE stop_lat >= %f AND stop_lat <= %f AND stop_lon >= %f AND stop_lon <= %f)
    GROUP BY %s)
    INSERT INTO %s
    SELECT %s FROM %s_raw
    INNER JOIN st
    USING (%s) WHERE dataset_id = md
    ''' % (TripTable().pks, StopTimesTable().name, StopTable().pks,
           StopTable().pks, StopTable().name, ONDER, BOVEN, LINKS, RECHTS,
           TripTable().pks,
           StopTimesTable().name,
           StopTimesTable().fieldnames, StopTimesTable().name,
           TripTable().pks)
    print sql
    with Connection() as con:
        con.execute(sql)
    # Stop Table Final
    print 'Creating final stop table'
    sql = '''
    INSERT INTO %s
    SELECT %s FROM %s_t
    WHERE %s IN (SELECT %s FROM %s)
    ''' % (StopTable().name, StopTable().fieldnames, StopTable().name,
           StopTable().pks, StopTable().pks, StopTimesTable().name)
    with Connection() as con:
        con.execute(sql)
    # Trip Table
    print 'Creating trip table'
    sql = '''
    INSERT INTO %s
    SELECT DISTINCT ON (%s) %s FROM %s_raw
    WHERE %s IN (SELECT %s FROM %s)
    ORDER BY %s, dataset_id DESC
    ''' % (TripTable().name, TripTable().pks, TripTable().fieldnames,
           TripTable().name, TripTable().pks, TripTable().pks, StopTimesTable().name, TripTable().pks)
    with Connection() as con:
        con.execute(sql)
    # Shapes Table
    print 'Creating shapes table'
    sql = '''
    INSERT INTO %s
    SELECT DISTINCT ON (%s) %s FROM %s_raw
    WHERE shape_id IN (SELECT shape_id FROM %s)
    ORDER BY %s, dataset_id DESC
    ''' % (ShapesTable().name, ShapesTable().pks, ShapesTable().fieldnames, ShapesTable().name,
           TripTable().name, ShapesTable().pks)
    with Connection() as con:
        con.execute(sql)
    # Routes Table
    print 'Creating routes table'
    sql = '''
    INSERT INTO %s
    SELECT DISTINCT ON (%s) %s FROM %s_raw
    WHERE %s IN (SELECT %s FROM %s)
    ORDER BY %s, dataset_id DESC
    ''' % (RoutesTable().name, RoutesTable().pks, RoutesTable().fieldnames,
           RoutesTable().name, RoutesTable().pks, RoutesTable().pks, TripTable().name, RoutesTable().pks)
    with Connection() as con:
        con.execute(sql)
    # Transfer Table
    print 'Creating transfer table'
    sql = '''
    WITH s AS (SELECT %s FROM %s),
    t AS (SELECT %s FROM %s)
    INSERT INTO %s
    SELECT DISTINCT ON (%s) %s FROM %s_raw
    WHERE from_stop_id IN (SELECT * FROM s) AND to_stop_id IN (SELECT * FROM s)
    AND from_trip_id IN (SELECT * FROM t) AND to_trip_id IN (SELECT * FROM t)
    ORDER BY %s, dataset_id DESC
    ''' % (StopTable().pks, StopTable().name, TripTable().pks, TripTable().name, TransferTable().name,
           TransferTable().pks, TransferTable().fieldnames, TransferTable().name, TransferTable.pks)
    with Connection() as con:
        con.execute(sql)
    # Service Table
    print 'Creating service table'
    sql = '''
    INSERT INTO %s
    SELECT DISTINCT ON (%s) %s FROM %s_raw
    WHERE service_id IN (SELECT service_id FROM %s)
    ORDER BY %s, dataset_id DESC
    ''' % (CalendarDatesTable().name, CalendarDatesTable().pks, CalendarDatesTable().fieldnames,
           CalendarDatesTable().name, TripTable().name, CalendarDatesTable().pks)
    with Connection() as con:
        con.execute(sql)
    # Agency Table
    print 'Creating agency table'
    sql = '''
    INSERT INTO %s
    SELECT DISTINCT ON (%s) %s FROM %s_raw
    WHERE %s IN (SELECT %s FROM %s)
    ORDER BY %s, dataset_id DESC
    ''' % (AgencyTable().name, AgencyTable().pks, AgencyTable().fieldnames,
           AgencyTable().name, AgencyTable().pks, AgencyTable().pks, RoutesTable().name, AgencyTable().pks)
    with Connection() as con:
        con.execute(sql)
    # Feed info table
    print ' Creating feed info table'
    sql = '''
    INSERT INTO feed_info
    SELECT feed_publisher_name, feed_id, feed_publisher_url, feed_lang,
    min(feed_start_date), max(feed_end_date), max(feed_version) FROM feed_info_raw
    GROUP BY feed_publisher_name, feed_id, feed_publisher_url, feed_lang
    '''
    with Connection() as con:
        con.execute(sql)





def createUniqueTables():
    # dropTables("_u")
    # createTables("_u", True)
    # for table in [AgencyTable(), CalendarDatesTable(), FeedInfoTable(), RoutesTable(),
    #               StopTable, TransferTable, TripTable]:
    #     print "Processing table %s" % table.name
    #     sql = "INSERT INTO %s_u (%s) " \
    #           "SELECT DISTINCT ON (%s) %s " \
    #           "FROM %s_raw ORDER BY %s, dataset_id DESC" \
    #           % (table.name, table.fieldnames, table.pks, table.fieldnames, table.name, table.pks)
    #     with Connection() as con:
    #         con.execute(sql)
    # print 'Processing table shapes'
    # sql = '''
    # INSERT INTO shapes_u (shape_id, shape_pt_sequence, shape_pt_lat, shape_pt_lon, shape_dist_traveled)
    # SELECT t.shape_id shape_id, shape_pt_sequence, shape_pt_lat, shape_pt_lon, shape_dist_traveled
    # FROM shapes_raw as s
    # INNER JOIN
    # (SELECT DISTINCT ON (shape_id) shape_id, dataset_id
    # FROM shapes_raw
    # ORDER BY shape_id, dataset_id DESC) as t
    # ON s.shape_id = t.shape_id AND s.dataset_id = t.dataset_id
    # '''
    # with Connection() as con:
    #     con.execute(sql)
    with Connection() as con:
        con.execute('DROP TABLE trips_u')
        con.execute(TripTable().getCreateStatement("_u", True, True))
    table = TripTable()
    print "Processing table %s" % table.name
    sql = "INSERT INTO %s_u (%s, dataset_id) " \
          "SELECT DISTINCT ON (%s) %s, dataset_id " \
          "FROM %s_raw ORDER BY %s, dataset_id DESC" \
          % (table.name, table.fieldnames, table.pks, table.fieldnames, table.name, table.pks)
    with Connection() as con:
        con.execute(sql)
    print 'Processing table stop_times'
    sql = '''
    INSERT INTO stop_times_u (%s)
    SELECT %s FROM stop_times_raw
    INNER JOIN trips_u USING (trip_id, dataset_id)
    ''' % (StopTimesTable().fieldnames, StopTimesTable().fieldnames)
    with Connection() as con:
        con.execute('DROP TABLE stop_times_u')
        con.execute(StopTimesTable().getCreateStatement('_u', True))
        con.execute(sql)
    sql = '''
    INSERT INTO shapes_u (%s)
    SELECT %s FROM shapes_raw
    INNER JOIN trips_u USING (shape_id, dataset_id)
    ''' % (ShapesTable().fieldnames, ShapesTable().fieldnames)
    with Connection() as con:
        con.execute('DROP TABLE shapes_u')
        con.execute(ShapesTable().getCreateStatement("_u", True))
        con.execute(sql)



def clipTables():
    dropTables()
    createTables()

    print 'Processing stops'
    sql = '''
    INSERT INTO stops
    SELECT %s FROM stops_u
    WHERE stop_lat >= %f AND stop_lat <= %f AND stop_lon >= %f AND stop_lat <= %f;
    ''' % (StopTable().fieldnames, ONDER, BOVEN, LINKS, RECHTS)
    with Connection(autocommit=False) as con:
        con.execute(sql)
        con.commit()
        con.execute('VACUUM stops')
        con.commit()
    print 'Processing stop_times'
    sql = '''
    INSERT INTO stop_times
    SELECT %s FROM stop_times_u WHERE trip_id IN
    (SELECT trip_id FROM stop_times_u
    WHERE stop_id in
    (SELECT stop_id FROM stops) AS s
    GROUP BY trip_id) AS st;
    '''
    with Connection(autocommit=False) as con:
        con.execute(sql)
        con.commit()
        con.execute('VACUUM stop_times')
        con.commit()

    print 'Processing table trips'
    sql = '''
    WITH s AS (SELECT stop_id FROM stops_u
    WHERE stop_lat >= %f AND stop_lat <= %f AND stop_lon >= %f AND stop_lat <= %f),
    st AS (SELECT trip_id
    '''





