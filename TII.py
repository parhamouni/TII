## THIS CODE IS WRITTEN BY PARHAM HAMOUNI TO IMPROVE TII FOR TRIP LAB
## JUNE 26 2017

import psycopg2
import pandas as pd
import numpy as np
import datetime
import gpxpy.geo
import time
import matplotlib.pyplot as plt

try:
        conn = psycopg2.connect("dbname='gtfs_stm_oct_2016' user='postgres' host='localhost' password= 'postgres' ")
except:
        print "I am unable to connect to the database"
cur = conn.cursor()


def input_extract(user, ipere):
    """Assumes that user is a string which is the user_id of traveller and ipere is an integer which is the unique trip id
    Returns a dataframe in which the possible bus routes within 15 meters are determined in the column called routelist"""

    tables_for_dropping_beg = ['input_temp_order', 'input_route_order', 'routelist_temp', 'input_temp_routelist']
    for table in tables_for_dropping_beg:
        sql_temp = """
                      DROP TABLE IF EXISTS {};
                      """.format(table)
        cur.execute(sql_temp)
        conn.commit()

    sql_temp = """
        SELECT DISTINCT on (id) * INTO input_temp_order FROM Amir_data_zak WHERE user_id='{}' and ipere = '{}' ORDER BY id;  
        Select * FROM input_temp_order order by times;""".format(str(user), ipere)
    cur.execute(sql_temp)
    conn.commit()
    query = """
    select t.id, array_agg(distinct(numero_lig)) as routelist into routelist_temp from (SELECT *
    FROM input_temp_order s   
    LEFT JOIN amir_gtfs r ON ST_DWithin(s.geom, r.geom, 15) ORDER BY s.id) as t group by t.id;"""
    cur.execute(query)
    conn.commit()
    query1 = '''SELECT
    input_temp_order.* , routelist_temp.routelist  into input_temp_routelist
    FROM
     input_temp_order
    inner join routelist_temp ON input_temp_order.id = routelist_temp.id;
    Select * from input_temp_routelist;'''
    cur.execute(query1)
    conn.commit()
    # global ct
    ct = cur.fetchall()

    col_names = []
    for elt in cur.description:
        col_names.append(elt[0])

    df = pd.DataFrame(ct,index=list(range(len(ct))), columns=col_names)

    time_threshold = datetime.timedelta(minutes=5) ## Reference: Amir's article
    df['Time_Difference'] = df['times'] - df['times'].shift(-1)
    df['OVER 5 MINS'] = (df['Time_Difference'] > time_threshold)
    df['Distance_Between'] = None
    df['Average_Speed (Km/hr)'] = None
    df['Metro_Detected'] = None
    df['Segment_Detected'] = np.nan
    df['inferred_1'] = None # The outputs are inferred from bus_line_detection(df)
    df['inferred_2'] = None # The rows are filled with bus_line_detection_auto_segment(df)
    df['Ambiguity_bus'] = None
    df['busnum'] = None
    df['inferred_3'] = None  # the outputs are combination of metro detection and bus_line_detection_auto_segment(df) outputs under certain conditions
    df['Ambiguity_metro']= None
    df['inferred_4'] = None # the outputs are combination of metro detection and bus_line_detection_auto_segment(df) outputs and walking process under certain conditions
    df['Ambiguity_walk'] = None
    df['inferred_5'] = None #the outputs are the final results
    df['Ambiguity_time_process'] = None
    df['Validation'] = None

    for i in range(len(df)-1):
        lon_1 = df.loc[i, "longitude"]
        lon_2 = df.loc[i+1, "longitude"]
        lat_1 = df.loc[i, 'latitude']
        lat_2 = df.loc[i+1, 'latitude']
        dist = gpxpy.geo.haversine_distance(lat_1, lon_1, lat_2, lon_2)
        df.set_value(i, 'Distance_Between', dist)

    for i in range(len(df)-1):
        time_seconds = (df.loc[i, 'Time_Difference']).total_seconds()
        try:
            speed = (df.loc[i,'Distance_Between']/time_seconds)*3.6
        except:
            speed = np.nan
        df.set_value(i, 'Average_Speed (Km/hr)', speed)

    return df

def bus_line_detection(df):
    """Assumes df is a dataframe of a trip, which is output of A SEGMENTED input_extract;
    Returns a dataframe with a new column called inferred_1, which is the intersection of all row sets of corresponding route list column"""

    df = df
    bus_list=[]
    # THE SEGMENT AND MODES COLUMNS NEEDS TO BE MODIFIED WHEN SEGMENT ALGORITHM IS IMPLEMENTED ###
    df_seg_ref = df[(~df.duplicated('segment'))]
    distinct_bus_segment = list(df_seg_ref['segment'].values)
    distinct_bus_segment = filter(None, distinct_bus_segment)
    if distinct_bus_segment == []:
        distinct_bus_segment=[0]
    try:
        if math.isnan(distinct_bus_segment[0]):
            distinct_bus_segment = [0]
    except:
        pass

    for j in distinct_bus_segment:

        df_segment = df[df['segment']==j].copy()
        for i in list((df_segment).index.values):
            bus_pre = df['routelist'].values[i]
            bus_list.append(bus_pre)
        bus = map(set, bus_list)
        result = bus[0]
        for i in range(len(df_segment)):
            result = result.intersection(bus[i])
        result = list(result)
        df['inferred_1'] = df['inferred_1'].astype(object)
        for i in list((df_segment).index.values):
            df.set_value(i, 'inferred_1', result)

    return df


def intersector(key_start,end_key, bus_candidate_set):

    result = bus_candidate_set[key_start]
    bus_candidate_list =list(bus_candidate_set)
    result_list = []
    for (i,lines) in enumerate(bus_candidate_list[key_start:end_key]):
        result = result.intersection(set(lines))
        result_list.append(list(result))
    return result_list

def bus_line_detection_auto_segment(df):

    """Assumes df is a dataframe of a trip, which is output of A SIMPLE input_extract WITHOUT SEGMENTATION, it does it automatically;
       Returns a dataframe with a new column called inferred_2, which is the intersection of all row sets of corresponding route list column"""
    bus_list = []
    for i in list((df).index.values):
        bus_pre = df['routelist'].values[i]
        bus_list.append(bus_pre)
    bus = map(set, bus_list)
    ## if you want to activate bus processing in the beginning of the algorithm, please uncomment the following commands##
    # bus_union = set().union(*bus)
    # bus_union_filtered = filter(None, bus_union)
    # df_start = df1.head(1)
    # start_time = (df_start['timestamp'].values)[0]
    # start_time_convert = datetime.datetime.utcfromtimestamp(start_time.tolist() / 1e9)
    # start_time_seg_sec = (start_time_convert.hour * 3600 + start_time_convert.minute * 60 + start_time_convert.second)
    # df_end = df1.tail(1)
    # end_time = (df_end['timestamp'].values)[0]
    # end_time_convert = datetime.datetime.utcfromtimestamp(end_time.tolist() / 1e9)
    # end_time_seg_sec = (end_time_convert.hour * 3600 + end_time_convert.minute * 60 + end_time_convert.second)
    #
    # line_possible = []
    # for line_number in bus_union_filtered:
    #     try:
    #         start_line = line_time_results[line_number][0]
    #         end_line = line_time_results[line_number][1]
    #         if ((start_line < start_time_seg_sec) & (end_line > end_time_seg_sec)):
    #             line_possible.append(line_number)
    #     except:
    #         line_time_extractor(line_number)
    #         start_line = line_time_results[line_number][0]
    #         end_line = line_time_results[line_number][1]
    #         if ((start_line < start_time_seg_sec) & (end_line > end_time_seg_sec)):
    #             line_possible.append(line_number)
    #
    # bus_lines_final = []
    # for bus_lines in bus_list:
    #     bus_lines_possible = []
    #     for buses in bus_lines:
    #         if buses in line_possible:
    #             bus_lines_possible.append(buses)
    #         else:
    #             pass
    #     bus_lines_final.append(bus_lines_possible)
    # bus = map(set, bus_union_filtered)

    key0 = 0
    segment_id = 0
    global segmentation
    segmentation = []
    while key0 < len(bus):
        raw_lines = intersector(key0,len(bus), bus)
        try:
            segment_index = raw_lines.index([])
        except:
            segment_index = len(raw_lines)

        try:
            result = raw_lines[segment_index - 1]
        except:
            result = raw_lines[0]
        segmentation.append([segment_id, key0, result])
        segment_id += 1

        for i in range(key0, len(bus)):
            df.set_value(i, 'Segment_Detected', segmentation[-1][0])
            df.set_value(i, 'inferred_2', segmentation[-1][2])
        key0 = key0 + segment_index + 1
    return df


def ambiguity_bus(df):

        for i in range(len(df)):
            try:
                if len(df.inferred_2[i]) < 2:
                    df.loc[i, 'Ambiguity_bus'] = 0
                else:
                    df.loc[i, 'Ambiguity_bus'] = 1
            except TypeError: # this is due None in inferred_2
                df.loc[i, 'Ambiguity_bus'] = 1

            try:
                df.loc[i, 'routecount'] = len(df.loc[i, 'routelist'])  # Showing that this row(or route) is not ambiguous
                if len(df.loc[i, 'routelist']) < 2:
                    df.loc[i, 'numroute'] = 0  # Showing that this row(or route) is not ambiguous
                else:
                    df.loc[i, 'numroute'] = 1
                df.loc[i,'num_inferred2'] = len(df.inferred_2[i])
            except TypeError:
                df.loc[i,'num_inferred2'] = 0

        return df


def metro_processing(df):

    df_metro = df.loc[(df["metro"] == 1) & (df["OVER 5 MINS"] == True), :]
    df.Metro_Detected =df.Metro_Detected.astype("O")
    list_of_metro_ids = []
    for i in df_metro.index:
        list_of_metro_ids.append(i)
        list_of_metro_ids.append(i+1)
    global metro_ids
    metro_ids = list(set(list_of_metro_ids))

    for i in metro_ids:
        sub_df = df.loc[i, "station"]
        df.loc[i, 'Metro_Detected'] = sub_df

    for i in range(len(df)):
        if df.Metro_Detected[i] is not None:
            metro_list_convert = []
            metro_list_convert.append(df.loc[i, 'Metro_Detected'])
            df.set_value(i, 'inferred_3', metro_list_convert)
            df.set_value(i, 'Segment_Detected', (df.loc[i,'Segment_Detected']+100))
        else:
            df.set_value(i,'inferred_3', df.loc[i , 'inferred_2'])

    return df


def ambiguity_Metro(df):

    for i in range(len(df)):
        try:
            df.loc[i,'num_inferred3'] = len(df.loc[i,'inferred_3'])
            if len(df.loc[i,'inferred_3']) == 1:
                df.loc[i,'Ambiguity_metro'] =  0    # Showing that this row(or route) is not ambiguous
            else:
                df.loc[i, 'Ambiguity_metro'] = 1
        except TypeError:
            df.loc[i, 'Ambiguity_metro'] = 1

    return df


def line_time_extractor( line_number ):
    if line_number is None:
        pass
    else:
        query = "select route_id, array_agg(distinct(trip_id)) as distinct_ids from trip_shapes where route_id = {} group by trip_shapes.route_id;".format(line_number)
        cur.execute(query)
        conn.commit()
        reference_list = cur.fetchall()
        try:
            trip_list = reference_list[0][1]
            all_times_list = []
            for trip_id in trip_list:
                query = """select min(arrival_time), max(arrival_time) from stop_times where trip_id = '{}';""".format(trip_id)
                cur.execute(query)
                conn.commit()
                times_list = cur.fetchall()
                all_times_list.append(times_list[0][0])
                all_times_list.append(times_list[0][1])
            start_time = time.gmtime(min(all_times_list).total_seconds())
            start_time_seconds = start_time.tm_hour * 60 * 60 + start_time.tm_min * 60 + start_time.tm_sec
            end_time = time.gmtime(max(all_times_list).total_seconds())
            end_time_seconds = end_time.tm_hour * 60 * 60 + end_time.tm_min * 60 + end_time.tm_sec

            if end_time_seconds < start_time_seconds:
                end_time_seconds = end_time_seconds + 86400
            line_time_results[line_number] = [start_time_seconds, end_time_seconds]
        except:
            print line_number
            line_time_results[line_number] = [0,86400]

    return line_time_results


def walk_process(df):
    for i in range(len(df)):
        df.set_value(i, 'inferred_4', df.loc[i, 'inferred_3'])
        if df.loc[i, 'inferred_3'] is None:
            df.set_value(i, 'inferred_4', [None])

    # # These are my assumptions, not his!
    # for i in range(len(df)):
    #     if any(c in df.loc[i, 'inferred_4'] for c in [1, 2, None]) or df.loc[i, 'inferred_4'] == [None]:
    #         station_name = df.loc[i, 'station']
    #         df.set_value(i, 'inferred_4', [station_name])
    #     # except TypeError:
        #     df.drop(df.index[i])
### zak process
    unique_segments = df.Segment_Detected.unique()
    segmentation_inferred = []
    for i in unique_segments:
        segmentation_inferred.append([i, df[df.Segment_Detected == i].inferred_4.values[0]])

    for i in range(len(segmentation_inferred)-2):
        if segmentation_inferred[i][1] == segmentation_inferred[i + 2][1] and segmentation_inferred[i + 1][1] == [None] and segmentation_inferred[i][1] != [None]:
            indexes = df.loc[df.Segment_Detected == segmentation_inferred[i + 1][0]].index
            for index in indexes:
                df.set_value(index, 'inferred_4', segmentation_inferred[i][1])
                df.set_value(indexes, 'Segment_Detected', segmentation_inferred[i][0])

    unique_segments = df.Segment_Detected.unique()
    for segment in unique_segments:
        distance_segment = df[df.Segment_Detected == segment].Distance_Between.sum()
        if distance_segment > 200:
            pass
        else:
            indices = df.loc[df.Segment_Detected == segment, 'inferred_4'].index
            for i in indices:
                df.set_value(i, 'inferred_4', [None])
                df.set_value(i, 'Segment_Detected', (segment+200))

    for i in range(len(df)):
        if df.inferred_4[i] == []:
            df.set_value(i, 'inferred_4', [None])

    return df

def ambiguity_walk(df):
    for i in range(len(df)):
        try:
            df.loc[i, 'num_inferred4'] = len(df.loc[i, 'inferred_4'])
            if len(df.loc[i,'inferred_4']) < 2:
                df.loc[i,'Ambiguity_walk'] = 0    # Showing that this row(or route) is not ambiguous
            else:
                df.loc[i, 'Ambiguity_walk'] = 1
        except TypeError:
            df.loc[i, 'Ambiguity_walk'] = 1

    return df

def bus_time_processing(df):

    for i in range(len(df)):
        df.set_value(i,'inferred_5', df.loc[i,'inferred_4'])

    # I erased ambiguity condition in ambi_seg_list, the redundant parenthesis is caused by that intentionally

    ambi_seg_list = list(df.Segment_Detected.unique())

    for segment in ambi_seg_list:
        df_ambi_segment = df.loc[(df['Segment_Detected'] == segment)]
        df_start = df_ambi_segment.tail(1)

        # this is due to start and end and time!
        end_id = (df_start.index.values)[0]
        start_time = (df_start['times'].values)[0]
        start_time_convert = datetime.datetime.utcfromtimestamp(start_time.tolist() / 1e9)
        start_time_seg_sec = (start_time_convert.hour * 3600 + start_time_convert.minute * 60 + start_time_convert.second)
        df_end = df_ambi_segment.head(1)
        start_id = (df_end.index.values)[0]
        end_time = (df_end['times'].values)[0]
        end_time_convert = datetime.datetime.utcfromtimestamp(end_time.tolist() / 1e9)
        end_time_seg_sec = (end_time_convert.hour * 3600 + end_time_convert.minute * 60 + end_time_convert.second)

        routelist = df_start["inferred_4"].values[0]
        inferred_included =[]
        global line_number
        for line_number in routelist:
            if line_number is None or isinstance(line_number,str):
                pass
            else:
                try:
                    start_line = line_time_results[line_number][0]
                except:
                    line_time_extractor(line_number)
                    start_line = line_time_results[line_number][0]

                end_line = line_time_results[line_number][1]
                line_number_string = str(line_number)
                two_hundred_range = range(250,300)
                if (start_line<start_time_seg_sec) & (end_line>end_time_seg_sec):
                    if len(line_number_string)==3 and line_number_string[0]=='3':
                        pass
                    elif len(line_number_string)==3 and line_number_string[0]=='4':
                        pass
                    elif line_number in two_hundred_range:
                        pass
                    else:
                        inferred_included.append(line_number)

        for i in range(start_id,end_id+1):
            if df.loc[i,'inferred_4'] is None:
                pass
            elif isinstance(df.loc[i, 'inferred_4'][0],str):
                df.set_value(i, 'inferred_5', df.loc[i, 'inferred_4'])
            else:
                if len(inferred_included) == 0:
                    df.set_value(i, 'inferred_5', [None])
                else:
                    df.set_value(i, 'inferred_5', inferred_included)

    return df

def ambiguity_time(df):
    for i in range(len(df)):
        # try:
        #     if any(c in df.loc[i, 'inferred_3'] for c in [1, 2, None]) or df.loc[i, 'inferred_3'] == [None]:
        #         station_name = df.loc[i, 'station']
        #         df.set_value(i, 'inferred_4', [station_name])
        # except:
        #     station_name = df.loc[i, 'station']
        #     df.set_value(i, 'inferred_4', [station_name])

        try:
            df.loc[i, 'num_inferred_5']=len(df.loc[i, 'inferred_5'])
            if len(df.loc[i, 'inferred_5']) < 2:
                df.loc[i, 'Ambiguity_time_process'] = 0  # Showing that this row(or route) is not ambiguous
            else:
                df.loc[i, 'Ambiguity_time_process'] = 1
        except:
            df.drop(df.index[i])
    return df


def validation(df):
    for i in range(len(df)):
        try:
            if df.loc[i, 'Ambiguity_time_process'] == 1:
                df.loc[i, 'Validation'] = 0
            else:
                if isinstance((df.loc[i, 'inferred_5'][0]),str):
                    xstra = lambda s: s or ""
                    if 'Metro' in xstra(df.loc[i, 'modes']):
                        df.loc[i, 'Validation'] = 1
                    else:
                        df.loc[i, 'Validation'] = 0
                elif df.loc[i, 'inferred_5'] == [] or df.loc[i, 'inferred_5'] == [None]:
                    xstra = lambda s: s or ""
                    if 'alk' in xstra(df.loc[i, 'modes']):
                        df.loc[i, 'Validation'] = 1
                    else:
                        df.loc[i, 'Validation'] = 0
                else:
                    line_name = str((df.loc[i, 'inferred_5'])[0])
                    route_name = df.loc[i, 'route']
                    if line_name in route_name:
                        df.loc[i, 'Validation'] = 1
                    else:
                        df.loc[i, 'Validation'] = 0
        except:
                df.loc[i, 'Validation'] = 0
    return df

line_time_results = {5: [19800, 20715],
 10: [18540, 93240],
 11: [24000, 88500],
 12: [21600, 84420],
 14: [21600, 70500],
 15: [32400, 90000],
 16: [21300, 79020],
 17: [17460, 92520],
 18: [17400, 95100],
 19: [71760, 89880],
 21: [23400, 35520],
 24: [18660, 95700],
 25: [23820, 68940],
 26: [18000, 81060],
 27: [18300, 93360],
 28: [18000, 96360],
 29: [20700, 91980],
 30: [17400, 91440],
 32: [17880, 95160],
 33: [16740, 95100],
 34: [17880, 93600],
 36: [17100, 90120],
 37: [18420, 95520],
 41: [19800, 78900],
 43: [16980, 95340],
 44: [17220, 95400],
 45: [17400, 94920],
 47: [18960, 93540],
 48: [15660, 95760],
 49: [16200, 95340],
 51: [17280, 94920],
 52: [22800, 64680],
 53: [18540, 93480],
 54: [19680, 71520],
 55: [18180, 92100],
 57: [19800, 90000],
 58: [18000, 93960],
 61: [19560, 91740],
 63: [22500, 70800],
 64: [18360, 93300],
 66: [18720, 93660],
 67: [16740, 94680],
 68: [17400, 95280],
 69: [17100, 95340],
 70: [21600, 93060],
 71: [18000, 82560],
 72: [19800, 71880],
 73: [23400, 66300],
 74: [20820, 73380],
 75: [26640, 66540],
 76: [79560, 87960],
 77: [21600, 75420],
 78: [18000, 92940],
 80: [18900, 94500],
 85: [17280, 94680],
 90: [17700, 94500],
 92: [18420, 94320],
 93: [18420, 93000],
 94: [18780, 95760],
 95: [17940, 94380],
 97: [16620, 94980],
 99: [18480, 93840],
 100: [18300, 91980],
 102: [19800, 95400],
 103: [19200, 94260],
 104: [19800, 94860],
 105: [18900, 93900],
 106: [18000, 94440],
 107: [18000, 94080],
 108: [18000, 93660],
 110: [15960, 96480],
 112: [18000, 95220],
 113: [17100, 94920],
 115: [21600, 67980],
 117: [20700, 71460],
 119: [25080, 72300],
 121: [17400, 93780],
 123: [19800, 90060],
 124: [20100, 93300],
 125: [19200, 93300],
 126: [27900, 56100],
 128: [19620, 91020],
 129: [21600, 94260],
 131: [18900, 94260],
 135: [22980, 66300],
 136: [18240, 94620],
 138: [20820, 93540],
 139: [17700, 94740],
 140: [17760, 95520],
 141: [17100, 94920],
 144: [18600, 93660],
 146: [18780, 94080],
 150: [23400, 68400],
 160: [19080, 96060],
 161: [17700, 94800],
 162: [20700, 95160],
 164: [19500, 94980],
 165: [19500, 94200],
 166: [21600, 92100],
 168: [21600, 95640],
 171: [17880, 94800],
 174: [19800, 86340],
 178: [21600, 68460],
 183: [19320, 83280],
 185: [18900, 92400],
 188: [20580, 69240],
 191: [17160, 95580],
 192: [17100, 94320],
 193: [17700, 95460],
 195: [16500, 97260],
 196: [20700, 71520],
 197: [17820, 94200],
 200: [21000, 71940],
 201: [21300, 89580],
 202: [20400, 89280],
 203: [21300, 88500],
 204: [20820, 88920],
 208: [21840, 89160],
 209: [20580, 88560],
 211: [17700, 95760],
 213: [21060, 69300],
 216: [21600, 69600],
 217: [21180, 88080],
 218: [20220, 71160],
 219: [22200, 67620],
 220: [23100, 84900],
 225: [21360, 71820],
 252: [34200, 58380],
 253: [34200, 57300],
 254: [35100, 59280],
 256: [34500, 58440],
 257: [35940, 58740],
 258: [32400, 57060],
 259: [32400, 57300],
 260: [35100, 58500],
 350: [3180, 21060],
 353: [3600, 19500],
 354: [4740, 20700],
 355: [4740, 20700],
 356: [3600, 22860],
 357: [3600, 19260],
 358: [1800, 19380],
 359: [3600, 20220],
 360: [5400, 19560],
 361: [900, 18720],
 362: [5820, 20700],
 363: [780, 19920],
 364: [2940, 20160],
 365: [4800, 20940],
 368: [4920, 21000],
 369: [3600, 18540],
 370: [3600, 20520],
 371: [5400, 19260],
 372: [4500, 20940],
 376: [2700, 19980],
 378: [5040, 17220],
 380: [3840, 21000],
 382: [6300, 18780],
 401: [20340, 70500],
 405: [20940, 72540],
 406: [21720, 67980],
 407: [22740, 69960],
 409: [19800, 70620],
 410: [20820, 72660],
 411: [21660, 70140],
 419: [22200, 67860],
 420: [22500, 70680],
 425: [20400, 71700],
 427: [22320, 68340],
 430: [19620, 71640],
 432: [21060, 71040],
 435: [21840, 73560],
 439: [21600, 68460],
 440: [20520, 71040],
 448: [20460, 70860],
 449: [19500, 82680],
 460: [17820, 73980],
 467: [21720, 70380],
 468: [19980, 71100],
 469: [19440, 70680],
 470: [16620, 96300],
 475: [21600, 68100],
 485: [19560, 73320],
 491: [20700, 69420],
 495: [18000, 94440],
 496: [20700, 82020],
 715: [25200, 79560],
 747: [17400, 19260],
 769: [21600, 88980]}


query = """
select user_id, array_agg(distinct(ipere)) as iperes from Amir_data_zak where ipere != '99999' group by user_id;
"""

cur.execute(query)
tii_feed = cur.fetchall()

df_final = pd.DataFrame()
count = 0
starttime = datetime.datetime.now()
Number_of_iterations = 10

print "Start time is", starttime
ambiguity = []
validation_final = []
validation_df = []
ambiguity_df=[]
results_final = []
# while True:
for row in tii_feed:
    for ipere in row[1]:
        if ipere is None:
            pass
        else:
            df = input_extract(row[0],ipere)
            length = len(df)
            if length <= 1:
                pass
            else:
                print 'TII is running for user', row[0], "ipere is", ipere ,'for',length,'rows'
                df = bus_line_detection_auto_segment(df)
                df = ambiguity_bus(df)
                df = metro_processing(df)
                df = ambiguity_Metro(df)
                # df_bus = bus_line_detection(df)
                df = walk_process(df)
                df = ambiguity_walk(df)
                df = bus_time_processing(df)
                df = ambiguity_time(df)
                df = validation(df)
                df_final= pd.concat([df_final, df], ignore_index=True)
                count = count + 1

                distance_validated = df.loc[df['Validation'] == 1, 'Distance_Between'].sum()
                total_distance = df.loc[:, 'Distance_Between'].sum()
                validation_percent_df = distance_validated / total_distance * 100
                ambiguity_rate_df = df.loc[df['Ambiguity_time_process'] == 1, 'Distance_Between'].sum() / total_distance * 100
                validation_df.append(validation_percent_df)
                ambiguity_df.append(ambiguity_rate_df)
                results_final.append([count, length, validation_percent_df, ambiguity_rate_df])
                print "Iteration number is ", count
                now = datetime.datetime.now()
                print "time is ",now

#                 if (count>Number_of_iterations): break
#     if (count > Number_of_iterations): break
# if (count> Number_of_iterations):break
endtime = datetime.datetime.now()
Time_difference = (endtime - starttime).total_seconds()
distance_validated_final = df_final.loc[df_final['Validation'] == 1, 'Distance_Between'].sum()
total_distance_final = df_final.loc[:, 'Distance_Between'].sum()
validation_percent = distance_validated_final / total_distance_final * 100
df_final_ambiguity = df_final.loc[df_final['Ambiguity_time_process'] == 1, 'Distance_Between']
ambiguity_rate = df_final_ambiguity.sum() / total_distance_final * 100
print "Total running time was", Time_difference, "seconds for", count, "trips, with "
print 'Processing ',len(df_final),'rows', validation_percent ,'validated rate', 'with ambiguity rate of', ambiguity_rate
filename = 'output-{time}.csv'.format(time=datetime.datetime.now())
df_final.to_csv(filename)

inambiguity = []
for i in ambiguity_df:
    inambiguity.append(100 - i)
# ambiguity process
base_line_ambiguity = df_final.loc[df_final['numroute'] == 1, 'Distance_Between'].sum() / total_distance_final * 100
base_line_ambiguity__2 = df_final.loc[
                             df_final['routecount'] == 2, 'Distance_Between'].sum() / total_distance_final * 100
base_line_ambiguity__3 = df_final.loc[
                             df_final['routecount'] == 3, 'Distance_Between'].sum() / total_distance_final * 100
base_line_ambiguity_greater_than_3 = df_final.loc[df_final[
                                                      'routecount'] > 3, 'Distance_Between'].sum() / total_distance_final * 100
# bus ambiguity calculation
bus_line_ambiguity = df_final.loc[
                         df_final['Ambiguity_bus'] == 1, 'Distance_Between'].sum() / total_distance_final * 100
bus_line_ambiguity__2 = df_final.loc[
                            df_final['num_inferred2'] == 2, 'Distance_Between'].sum() / total_distance_final * 100
bus_line_ambiguity__3 = df_final.loc[
                            df_final['num_inferred2'] == 3, 'Distance_Between'].sum() / total_distance_final * 100
bus_line_ambiguity_greater_than_3 = df_final.loc[df_final[
                                                     'num_inferred2'] > 3, 'Distance_Between'].sum() / total_distance_final * 100

# metro ambiguity calculation
metro_ambiguity = df_final.loc[
                      df_final['Ambiguity_metro'] == 1, 'Distance_Between'].sum() / total_distance_final * 100
metro_ambiguity__2 = df_final.loc[
                         df_final['num_inferred3'] == 2, 'Distance_Between'].sum() / total_distance_final * 100
metro_ambiguity__3 = df_final.loc[
                         df_final['num_inferred3'] == 3, 'Distance_Between'].sum() / total_distance_final * 100
metro_ambiguity_greater_than_3 = df_final.loc[df_final[
                                                  'num_inferred3'] > 3, 'Distance_Between'].sum() / total_distance_final * 100

# walk ambiguity calculation
walk_ambiguity = df_final.loc[
                     df_final['Ambiguity_walk'] == 1, 'Distance_Between'].sum() / total_distance_final * 100
walk_ambiguity__2 = df_final.loc[
                        df_final['num_inferred4'] == 2, 'Distance_Between'].sum() / total_distance_final * 100
walk_ambiguity__3 = df_final.loc[
                        df_final['num_inferred4'] == 3, 'Distance_Between'].sum() / total_distance_final * 100
walk_ambiguity_greater_than_3 = df_final.loc[df_final[
                                                 'num_inferred4'] > 3, 'Distance_Between'].sum() / total_distance_final * 100

# time ambiguity calculation
time_ambiguity = df_final.loc[
                     df_final['Ambiguity_time_process'] == 1, 'Distance_Between'].sum() / total_distance_final * 100
time_ambiguity__2 = df_final.loc[
                        df_final['num_inferred_5'] == 2, 'Distance_Between'].sum() / total_distance_final * 100
time_ambiguity__3 = df_final.loc[
                        df_final['num_inferred_5'] == 3, 'Distance_Between'].sum() / total_distance_final * 100
time_ambiguity_greater_than_3 = df_final.loc[df_final[
                                                 'num_inferred_5'] > 3, 'Distance_Between'].sum() / total_distance_final * 100

ambiguity_results_df = pd.DataFrame(
    index=['Baseline', 'Bus route', 'Bus route + metro', 'Bus route + metro + walk',
           'Final ambiguity processing'],
    columns=['% ambiguity', '% 2 lines', '% 3 lines', '% > 3 lines'])
ambiguity_results_df.set_value('Baseline', ['% ambiguity', '% 2 lines', '% 3 lines', '% > 3 lines'],
                               [base_line_ambiguity, base_line_ambiguity__2, base_line_ambiguity__3,
                                base_line_ambiguity_greater_than_3])
ambiguity_results_df.set_value('Bus route', ['% ambiguity', '% 2 lines', '% 3 lines', '% > 3 lines'],
                               [bus_line_ambiguity, bus_line_ambiguity__2, bus_line_ambiguity__3,
                                bus_line_ambiguity_greater_than_3])
ambiguity_results_df.set_value('Bus route + metro', ['% ambiguity', '% 2 lines', '% 3 lines', '% > 3 lines'],
                               [metro_ambiguity, metro_ambiguity__2, metro_ambiguity__3,
                                metro_ambiguity_greater_than_3])
ambiguity_results_df.set_value('Bus route + metro + walk',
                               ['% ambiguity', '% 2 lines', '% 3 lines', '% > 3 lines'],
                               [walk_ambiguity, walk_ambiguity__2, walk_ambiguity__3,
                                walk_ambiguity_greater_than_3])
ambiguity_results_df.set_value('Final ambiguity processing',
                               ['% ambiguity', '% 2 lines', '% 3 lines', '% > 3 lines'],
                               [time_ambiguity, time_ambiguity__2, time_ambiguity__3,
                                time_ambiguity_greater_than_3])
results_df = pd.DataFrame(results_final,columns=['Number of trip','Number of rows of trip','Validation Rate Percent','Ambiguity Percent'])
results_df['Un-ambiguity Percent'] = pd.Series(inambiguity, index=results_df.index)
filename = 'Descriptive results ' + filename
results_df.to_csv(filename)
filename = 'Ambiguity'+ filename
ambiguity_results_df.to_csv(filename)



# bins = range(0,105,5)
# # plt.xlim([min(bins)-5, max(bins)+5])
# plt.hist(inambiguity, bins = bins, alpha=0.5,label='Inambiguity of trips ')
# # plt.title('Inambiguity of trips (fixed bin size)')
# # plt.xlabel('Inambiguity percentage (bin size = 5)')
# # plt.ylabel('count')
#
# # bins = range(0,100,5)
# # plt.xlim([min(bins)-5, max(bins)+5])
# plt.hist(validation_df, bins=bins, alpha=0.5,label= 'Validation of trips')
# #
# # plt.title('Validation of trips (fixed bin size)')
# # plt.xlabel('Validation percentage (bin size = 5)')
# plt.ylabel('count')
# plt.legend(loc='upper right')
# plt.show()