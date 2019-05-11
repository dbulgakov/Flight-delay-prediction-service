import calendar
import requests
import zipfile
import io
import logging


__all__ = ['BTSFlightDataDownloader']


class BTSFlightDataDownloader(object):
    @property
    def __common_data_fields(self):
        return [
            ('UserTableName', 'On_Time_Performance'),
            ('DBShortName', 'On_Time'),
            ('RawDataTable', 'T_ONTIME'),
            ('varlist', (
                'FL_DATE,COP_UNIQUE_CARRIER,CTAIL_NUM'
                ',CORIGIN_AIRPORT_ID,CORIGIN_AIRPORT_SEQ_ID,CORIGIN_CITY_MARKET_ID'
                ',CORIGIN,CORIGIN_CITY_NAME,CDEST_AIRPORT_ID,CDEST_AIRPORT_SEQ_ID'
                ',CDEST_CITY_MARKET_ID,CDEST,CDEST_CITY_NAME,CCRS_DEP_TIME,CDEP_DELAY'
                ',CARR_DELAY,CCANCELLED,CCANCELLATION_CODE,CDIVERTED,CCRS_ELAPSED_TIME'
                ',CCARRIER_DELAY,CWEATHER_DELAY,CNAS_DELAY,CSECURITY_DELAY,CLATE_AIRCRAFT_DELAY'
            )),
            ('filter1', 'title='),
            ('filter2', 'title='),
            ('geo', 'All'),
            ('timename', 'Month'),
            ('GEOGRAPHY', 'All'),
            ('FREQUENCY', '1'),
            ('VarDesc', 'Year'),
            ('VarType', 'Num'),
            ('VarDesc', 'Quarter'),
            ('VarType', 'Num'),
            ('VarDesc', 'Month'),
            ('VarType', 'Num'),
            ('VarDesc', 'DayofMonth'),
            ('VarType', 'Num'),
            ('VarDesc', 'DayOfWeek'),
            ('VarType', 'Num'),
            ('VarName', 'FL_DATE'),
            ('VarDesc', 'FlightDate'),
            ('VarType', 'Char'),
            ('VarDesc', 'UniqueCarrier'),
            ('VarType', 'Char'),
            ('VarDesc', 'AirlineID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Carrier'),
            ('VarType', 'Char'),
            ('VarDesc', 'TailNum'),
            ('VarType', 'Char'),
            ('VarDesc', 'FlightNum'),
            ('VarType', 'Char'),
            ('VarDesc', 'OriginAirportID'),
            ('VarType', 'Num'),
            ('VarDesc', 'OriginAirportSeqID'),
            ('VarType', 'Num'),
            ('VarDesc', 'OriginCityMarketID'),
            ('VarType', 'Num'),
            ('VarName', 'ORIGIN'),
            ('VarDesc', 'Origin'),
            ('VarType', 'Char'),
            ('VarDesc', 'OriginCityName'),
            ('VarType', 'Char'),
            ('VarDesc', 'OriginState'),
            ('VarType', 'Char'),
            ('VarDesc', 'OriginStateFips'),
            ('VarType', 'Char'),
            ('VarDesc', 'OriginStateName'),
            ('VarType', 'Char'),
            ('VarDesc', 'OriginWac'),
            ('VarType', 'Num'),
            ('VarDesc', 'DestAirportID'),
            ('VarType', 'Num'),
            ('VarDesc', 'DestAirportSeqID'),
            ('VarType', 'Num'),
            ('VarDesc', 'DestCityMarketID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Dest'),
            ('VarType', 'Char'),
            ('VarDesc', 'DestCityName'),
            ('VarType', 'Char'),
            ('VarDesc', 'DestState'),
            ('VarType', 'Char'),
            ('VarDesc', 'DestStateFips'),
            ('VarType', 'Char'),
            ('VarDesc', 'DestStateName'),
            ('VarType', 'Char'),
            ('VarDesc', 'DestWac'),
            ('VarType', 'Num'),
            ('VarName', 'CRS_DEP_TIME'),
            ('VarDesc', 'CRSDepTime'),
            ('VarType', 'Char'),
            ('VarName', 'DEP_TIME'),
            ('VarDesc', 'DepTime'),
            ('VarType', 'Char'),
            ('VarDesc', 'DepDelay'),
            ('VarType', 'Num'),
            ('VarDesc', 'DepDelayMinutes'),
            ('VarType', 'Num'),
            ('VarDesc', 'DepDel15'),
            ('VarType', 'Num'),
            ('VarDesc', 'DepartureDelayGroups'),
            ('VarType', 'Num'),
            ('VarDesc', 'DepTimeBlk'),
            ('VarType', 'Char'),
            ('VarDesc', 'TaxiOut'),
            ('VarType', 'Num'),
            ('VarDesc', 'WheelsOff'),
            ('VarType', 'Char'),
            ('VarDesc', 'WheelsOn'),
            ('VarType', 'Char'),
            ('VarDesc', 'TaxiIn'),
            ('VarType', 'Num'),
            ('VarName', 'CRS_ARR_TIME'),
            ('VarDesc', 'CRSArrTime'),
            ('VarType', 'Char'),
            ('VarName', 'ARR_TIME'),
            ('VarDesc', 'ArrTime'),
            ('VarType', 'Char'),
            ('VarDesc', 'ArrDelay'),
            ('VarType', 'Num'),
            ('VarDesc', 'ArrDelayMinutes'),
            ('VarType', 'Num'),
            ('VarDesc', 'ArrDel15'),
            ('VarType', 'Num'),
            ('VarDesc', 'ArrivalDelayGroups'),
            ('VarType', 'Num'),
            ('VarDesc', 'ArrTimeBlk'),
            ('VarType', 'Char'),
            ('VarDesc', 'Cancelled'),
            ('VarType', 'Num'),
            ('VarDesc', 'CancellationCode'),
            ('VarType', 'Char'),
            ('VarDesc', 'Diverted'),
            ('VarType', 'Num'),
            ('VarDesc', 'CRSElapsedTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'ActualElapsedTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'AirTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'Flights'),
            ('VarType', 'Num'),
            ('VarDesc', 'Distance'),
            ('VarType', 'Num'),
            ('VarDesc', 'DistanceGroup'),
            ('VarType', 'Num'),
            ('VarDesc', 'CarrierDelay'),
            ('VarType', 'Num'),
            ('VarDesc', 'WeatherDelay'),
            ('VarType', 'Num'),
            ('VarDesc', 'NASDelay'),
            ('VarType', 'Num'),
            ('VarDesc', 'SecurityDelay'),
            ('VarType', 'Num'),
            ('VarDesc', 'LateAircraftDelay'),
            ('VarType', 'Num'),
            ('VarDesc', 'FirstDepTime'),
            ('VarType', 'Char'),
            ('VarDesc', 'TotalAddGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'LongestAddGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'DivAirportLandings'),
            ('VarType', 'Num'),
            ('VarDesc', 'DivReachedDest'),
            ('VarType', 'Num'),
            ('VarDesc', 'DivActualElapsedTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'DivArrDelay'),
            ('VarType', 'Num'),
            ('VarDesc', 'DivDistance'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div1Airport'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div1AirportID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div1AirportSeqID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div1WheelsOn'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div1TotalGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div1LongestGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div1WheelsOff'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div1TailNum'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div2Airport'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div2AirportID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div2AirportSeqID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div2WheelsOn'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div2TotalGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div2LongestGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div2WheelsOff'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div2TailNum'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div3Airport'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div3AirportID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div3AirportSeqID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div3WheelsOn'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div3TotalGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div3LongestGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div3WheelsOff'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div3TailNum'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div4Airport'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div4AirportID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div4AirportSeqID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div4WheelsOn'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div4TotalGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div4LongestGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div4WheelsOff'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div4TailNum'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div5Airport'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div5AirportID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div5AirportSeqID'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div5WheelsOn'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div5TotalGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div5LongestGTime'),
            ('VarType', 'Num'),
            ('VarDesc', 'Div5WheelsOff'),
            ('VarType', 'Char'),
            ('VarDesc', 'Div5TailNum'),
            ('VarType', 'Char')
        ]

    @property
    def __sql_query_filed(self):
        return ('sqlstr', (
            ' SELECT QUARTER,FL_DATE,OP_UNIQUE_CARRIER,TAIL_NUM,ORIGIN_AIRPORT_ID'
            ',ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,ORIGIN_CITY_NAME,DEST_AIRPORT_ID'
            ',DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,DEST_CITY_NAME,CRS_DEP_TIME,DEP_DELAY'
            ',ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,CRS_ELAPSED_TIME,CARRIER_DELAY'
            ',WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY FROM T_ONTIME'
            ' WHERE Month = {} AND YEAR = {}'
        ))

    @property
    def __headers_data(self):
        return {
            'Origin': 'https://www.transtats.bts.gov',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.8',
            'Upgrade-Insecure-Requests': '1',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Cache-Control': 'max-age=0',
            'Referer': 'https://www.transtats.bts.gov/DL_SelectFields.asp',
            'Connection': 'keep-alive',
        }

    @property
    def __cookies_data(self):
        return {
            'ASPSESSIONIDCAQCSDSS': 'GOBIGIDBLGILGICKLFHKIHMN',
            '__utmt_ritaTracker': '1',
            '__utmt_GSA_CP': '1',
            '__utma': '261918792.554646962.1504352085.1504442392.1504442407.3',
            '__utmb': '261918792.8.10.1504442407',
            '__utmc': '261918792',
            '__utmz': '261918792.1504442407.3.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)',
        }

    @property
    def __params_data(self):
        return (
            ('Table_ID', '236'),
            ('Has_Group', '3'),
            ('Is_Zipped', '0'),
        )

    @property
    def __request_url(self):
        return 'https://www.transtats.bts.gov/DownLoad_Table.asp'

    def __get_request_data(self, year, month):
        month_name = calendar.month_name[month]
        sql_query = (self.__sql_query_filed[0], self.__sql_query_filed[1].format(month, year))
        query_time = ('time', month_name)
        query_year = ('XYEAR', str(year))
        request_data = self.__common_data_fields[:]
        request_data.extend([sql_query, query_time, query_year])

        return request_data

    def download_fights_history(self, year, month):
        try:
            response = requests.get(self.__request_url,
                                    headers=self.__headers_data, params=self.__params_data,
                                    cookies=self.__cookies_data, data=self.__get_request_data(year, month))
            response_zip_data = zipfile.ZipFile(io.BytesIO(response.content))
            response_data = response_zip_data.open(response_zip_data.namelist()[0])

            data = response_data.read().decode("utf-8")
            response_zip_data.close()

            return data
        except Exception as e:
            logging.error(e)
