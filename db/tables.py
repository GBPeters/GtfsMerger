from db.pghandler import Connection


class DbTable (object):

    name = "template"

    sql = '''
    CREATE TABLE IF NOT EXISTS public.%s
    (
      %s
      %s
    )
    '''

    fields = '''
    dummy character varying,
    '''

    oidPk = '''
    oid bigserial NOT NULL,
    CONSTRAINT %s_pkey PRIMARY KEY (oid)
    '''

    nooidPk = '''
    CONSTRAINT %s_pkey PRIMARY KEY (%s)
    '''

    fieldnames = 'dummy'

    pks = 'dummy'

    def getCreateStatement(self, suffix="", withOid=False, withDatasetId=False):
        name = self.name + suffix
        fields = self.fields
        if withDatasetId:
            fields += 'dataset_id integer,'
        if withOid:
            pksql = self.oidPk % name
        else:
            pksql = self.nooidPk % (name, self.pks)
        sql = self.sql % (name, fields, pksql)
        return sql


class AgencyTable (DbTable):

    name = "agency"

    fields = '''
    agency_id character varying,
    agency_name character varying,
    agency_url character varying,
    agency_timezone character varying,
    agency_phone character varying,
    '''
    pks = 'agency_id'

    fieldnames = 'agency_id, agency_name, agency_url, agency_timezone, agency_phone'


class CalendarDatesTable (DbTable):

    name = 'calendar_dates'

    fields = '''
    service_id integer,
    date integer,
    exception_type smallint,
    '''

    fieldnames = 'service_id,date,exception_type'

    pks = 'service_id, date'


class FeedInfoTable (DbTable):

    name = 'feed_info'

    fields = '''
    feed_publisher_name character varying,
    feed_id character varying,
    feed_publisher_url character varying,
    feed_lang character varying,
    feed_start_date integer,
    feed_end_date integer,
    feed_version integer,
    '''

    fieldnames = 'feed_publisher_name,feed_id,feed_publisher_url,feed_lang,feed_start_date,feed_end_date,feed_version'

    pks = 'feed_publisher_name, feed_id, feed_version'


class RoutesTable (DbTable):

    name = 'routes'

    fields = '''
    route_id integer,
    agency_id character varying,
    route_short_name character varying,
    route_long_name character varying,
    route_desc character varying,
    route_type integer,
    route_color character varying,
    route_text_color character varying,
    route_url character varying,
    '''

    fieldnames = 'route_id,agency_id,route_short_name,route_long_name,' \
                 'route_desc,route_type,route_color,route_text_color,route_url'

    pks = 'route_id'


class ShapesTable (DbTable):

    name = 'shapes'

    fields = '''
    shape_id integer,
    shape_pt_sequence integer,
    shape_pt_lat double precision,
    shape_pt_lon double precision,
    shape_dist_traveled integer,
    '''

    fieldnames = 'shape_id,shape_pt_sequence,shape_pt_lat,shape_pt_lon,shape_dist_traveled'

    pks = 'shape_id, shape_pt_sequence'


class StopTimesTable (DbTable):

    name = 'stop_times'

    fields = '''
    trip_id integer,
    stop_sequence integer,
    stop_id character varying,
    stop_headsign character varying,
    arrival_time character varying,
    departure_time character varying,
    pickup_type integer,
    drop_off_type integer,
    timepoint integer,
    shape_dist_traveled integer,
    fare_units_traveled integer,
    '''

    fieldnames = 'trip_id,stop_sequence,stop_id,stop_headsign,arrival_time,departure_time,pickup_type,drop_off_type,' \
                 'timepoint,shape_dist_traveled,fare_units_traveled'

    pks = 'trip_id, stop_sequence'


class StopTable (DbTable):

    name = 'stops'

    fields = '''
    stop_id character varying,
    stop_code character varying,
    stop_name character varying,
    stop_lat double precision,
    stop_lon double precision,
    location_type integer,
    parent_station character varying,
    stop_timezone character varying,
    wheelchair_boarding integer,
    platform_code character varying,
    zone_id character varying,
    '''

    fieldnames = 'stop_id,stop_code,stop_name,stop_lat,stop_lon,location_type,parent_station,stop_timezone,' \
                 'wheelchair_boarding,platform_code,zone_id'

    pks = 'stop_id'


class TransferTable (DbTable):

    name = 'transfers'

    fields = '''
    from_stop_id character varying,
    to_stop_id character varying,
    from_route_id integer,
    to_route_id integer,
    from_trip_id integer,
    to_trip_id integer,
    transfer_type integer,
    '''

    fieldnames = 'from_stop_id,to_stop_id,from_route_id,to_route_id,from_trip_id,to_trip_id,transfer_type'

    pks = 'from_stop_id, to_stop_id, from_trip_id, to_trip_id'


class TripTable (DbTable):

    name = 'trips'

    fields = '''
    route_id integer,
    service_id integer,
    trip_id integer,
    realtime_trip_id character varying,
    trip_headsign character varying,
    trip_short_name character varying,
    trip_long_name character varying,
    direction_id integer,
    block_id integer,
    shape_id integer,
    wheelchair_accessible integer,
    bikes_allowed integer,
    '''

    fieldnames = 'route_id,service_id,trip_id,realtime_trip_id,trip_headsign,trip_short_name,trip_long_name,' \
                 'direction_id,block_id,shape_id,wheelchair_accessible,bikes_allowed'

    pks = 'trip_id'


class StopTripCrossTable (DbTable):

    name = "stop_trip_x"

    fields = '''
    stop_id character varying,
    trip_id integer,
    stop_sequence integer
    '''

    fieldnames = 'stop_id, trip_id, stop_sequence'

    pks = fieldnames


TABLES = [AgencyTable(), CalendarDatesTable(), FeedInfoTable(), RoutesTable(), ShapesTable(), StopTimesTable(),
          StopTable(), TransferTable(), TripTable()]

if __name__ == '__main__':
    print StopTimesTable().getCreateStatement()
    print StopTimesTable().getCreateStatement("_raw", True)


def dropTables(suffix=""):
    with Connection(autocommit=False) as con:
        for table in TABLES:
            sql = "DROP TABLE IF EXISTS public.%s%s" % (table.name, suffix)
            con.execute(sql)
        con.commit()


def dropTable(table, suffix=""):
    with Connection() as con:
        sql = "DROP TABLE IF EXISTS public.%s%s" % (table.name, suffix)
        con.execute(sql)


def createTables(suffix="", withOid=False):
    with Connection(autocommit=False) as con:
        for table in TABLES:
            sql = table.getCreateStatement(suffix, withOid)
            con.execute(sql)
        con.commit()

def createTable(table, suffix="", withOid=False):
    with Connection() as con:
        sql = table.getCreateStatement(suffix, withOid)
        con.execute(sql)