from __future__ import print_function
import blpapi
from collections import defaultdict
from pandas import DataFrame
from datetime import datetime, date, time, timedelta
import pandas as pd
import numpy as np
import sys
from pprint import pprint
import warnings
import six
from dateutil.relativedelta import relativedelta
import datetime
import pytz
import sqlalchemy
import logging
from blpapi.exception import InvalidArgumentException


class Pybbg():
    def __init__(self, host='localhost', port=8194):
        """
        Starting bloomberg API session
        close with session.close()
        """
        # Fill SessionOptions
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(host)
        sessionOptions.setServerPort(port)

        self.initialized_services = set()

        # Create a Session
        self.session = blpapi.Session(sessionOptions)

        # Start a Session
        if not self.session.start():
            print("Failed to start session.")

        self.session.nextEvent()

    def service_refData(self):
        """
        init service for refData
        """
        if '//blp/refdata' in self.initialized_services:
            return

        if not self.session.openService("//blp/refdata"):
            print("Failed to open //blp/refdata")

        self.session.nextEvent()

        # Obtain previously opened service
        self.refDataService = self.session.getService("//blp/refdata")

        self.session.nextEvent()

        self.initialized_services.add('//blp/refdata')

    def bdh(self, ticker_list, fld_list, start_date, end_date=date.today().strftime('%Y%m%d'), periodselection='DAILY', overrides=None, other_request_parameters=None, move_dates_to_period_end=False):
        """
        Get ticker_list and field_list
        return pandas multi level columns dataframe
        """
        # Create and fill the request for the historical data
        self.service_refData()

        if isstring(ticker_list):
            ticker_list = [ticker_list]
        if isstring(fld_list):
            fld_list = [fld_list]

        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y%m%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y%m%d')

        request = self.refDataService.createRequest("HistoricalDataRequest")
        for t in ticker_list:
            request.getElement("securities").appendValue(t)
        for f in fld_list:
            request.getElement("fields").appendValue(f)
        request.set("periodicitySelection", periodselection)
        request.set("startDate", start_date)
        request.set("endDate", end_date)


        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        if other_request_parameters is not None:
            for k,v in six.iteritems(other_request_parameters):
                request.set(k, v)

        def adjust_date(to_adjust):
            if periodselection == 'MONTHLY':
                # always make the date the last day of the month
                return date(to_adjust.year, to_adjust.month, 1) + relativedelta(months=1) - relativedelta(days=1)
            if periodselection == 'WEEKLY':
                return to_adjust + relativedelta(weekday=4)

            return to_adjust

        # print("Sending Request:", request)
        # Send the request
        self.session.sendRequest(request)
        # defaultdict - later convert to pandas
        data = defaultdict(dict)
        warnings.warn(str(data))
        # Process received events
        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                ticker = msg.getElement('securityData').getElement('security').getValue()
                fieldData = msg.getElement('securityData').getElement('fieldData')
                for i in range(fieldData.numValues()):
                    for j in range(1, fieldData.getValue(i).numElements()):
                        dt = fieldData.getValue(i).getElement(0).getValue()
                        if move_dates_to_period_end:
                            dt = adjust_date(dt)

                        data[(ticker, fld_list[j - 1])][dt] = fieldData.getValue(i).getElement(j).getValue()

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break

        if len(fld_list) == 1:
            data = {k[0]: v for k, v in data.items()}
            data = DataFrame(data)
            data = data[ticker_list]
            data.index = pd.to_datetime(data.index)
            return data

        if len(data) == 0:
            # security error case
            return DataFrame()

        data = DataFrame(data)
        data = data[ticker_list]
        data.columns = pd.MultiIndex.from_tuples(data, names=['ticker', 'field'])
        data.index = pd.to_datetime(data.index)
        return data

    def bdib(self, ticker, fld_list, startDateTime, endDateTime, eventType='TRADE', interval=1):
        """
        Get one ticker (Only one ticker available per call); eventType (TRADE, BID, ASK,..etc); interval (in minutes)
                ; fld_list (Only [open, high, low, close, volumne, numEvents] availalbe)
        return pandas dataframe with return Data
        """
        self.service_refData()
        # Create and fill the request for the historical data
        request = self.refDataService.createRequest("IntradayBarRequest")
        request.set("security", ticker)
        request.set("eventType", eventType)
        request.set("interval", interval)  # bar interval in minutes
        request.set("startDateTime", startDateTime)
        request.set("endDateTime", endDateTime)

        # print "Sending Request:", request
        # Send the request
        self.session.sendRequest(request)
        # defaultdict - later convert to pandas
        data = defaultdict(dict)
        # Process received events
        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                barTickData = msg.getElement('barData').getElement('barTickData')
                for i in range(barTickData.numValues()):
                    for j in range(len(fld_list)):
                        data[(fld_list[j])][barTickData.getValue(i).getElement(0).getValue()] = barTickData.getValue(
                            i).getElement(fld_list[j]).getValue()

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
        data = DataFrame(data)
        data.index = pd.to_datetime(data.index)
        return data

    def bdp(self, ticker, fld_list, overrides=None):
        # print(ticker, fld_list, overrides)
        self.service_refData()

        request = self.refDataService.createRequest("ReferenceDataRequest")
        if isstring(ticker):
            ticker = [ticker]

        securities = request.getElement("securities")
        for t in ticker:
            securities.appendValue(t)

        if isstring(fld_list):
            fld_list = [fld_list]

        fields = request.getElement("fields")
        for f in fld_list:
            fields.appendValue(f)

        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        self.session.sendRequest(request)
        data = dict()

        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                securityData = msg.getElement("securityData")

                for i in range(securityData.numValues()):
                    fieldData = securityData.getValue(i).getElement("fieldData")
                    secId = securityData.getValue(i).getElement("security").getValue()
                    data[secId] = dict()
                    for field in fld_list:
                        if fieldData.hasElement(field):
                            data[secId][field] = fieldData.getElement(field).getValue()
                        else:
                            data[secId][field] = np.NaN

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break

        return pd.DataFrame.from_dict(data)

    # def bdp(self, ticker, fld_list):

    #     self.service_refData()

    #     request = self.refDataService.createRequest("ReferenceDataRequest")
    #     if isstring(ticker):
    #         ticker = [ ticker ]

    #     securities = request.getElement("securities")
    #     for t in ticker:
    #         securities.appendValue(t)

    #     if isstring(fld_list):
    #         fld_list = [ fld_list ]

    #     fields = request.getElement("fields")
    #     for f in fld_list:
    #         fields.appendValue(f)


    #     self.session.sendRequest(request)
    #     data = dict()

    #     while(True):
    #         # We provide timeout to give the chance for Ctrl+C handling:
    #         ev = self.session.nextEvent(500)
    #         for msg in ev:
    #             securityData = msg.getElement("securityData")

    #             for i in range(securityData.numValues()):
    #                 fieldData = securityData.getValue(i).getElement("fieldData")
    #                 secId = securityData.getValue(i).getElement("security").getValue()
    #                 data[secId] = dict()
    #                 for field in fld_list:
    #                     if fieldData.hasElement(field):
    #                         data[secId][field] = fieldData.getElement(field).getValue()
    #                     else:
    #                         data[secId][field] = np.NaN



    #         if ev.eventType() == blpapi.Event.RESPONSE:
    #             # Response completly received, so we could exit
    #             break

    #     return pd.DataFrame.from_dict(data)


    def bds(self, security, field, overrides=None):

        self.service_refData()

        request = self.refDataService.createRequest("ReferenceDataRequest")
        assert isstring(security)
        assert isstring(field)

        securities = request.getElement("securities")
        securities.appendValue(security)

        fields = request.getElement("fields")
        fields.appendValue(field)

        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        # print(request)
        self.session.sendRequest(request)
        data = dict()

        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                # processMessage(msg)
                securityData = msg.getElement("securityData")
                for i in range(securityData.numValues()):
                    fieldData = securityData.getValue(i).getElement("fieldData").getElement(field)
                    for i, row in enumerate(fieldData.values()):
                        for j in range(row.numElements()):
                            e = row.getElement(j)
                            k = str(e.name())
                            v = e.getValue()
                            if k not in data:
                                data[k] = list()

                            data[k].append(v)

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break

        return pd.DataFrame.from_dict(data)

    def stop(self):
        self.session.stop()


def isstring(s):
    # if we use Python 3
    if (sys.version_info[0] == 3):
        return isinstance(s, str)
    # we use Python 2
    return isinstance(s, basestring)


def processMessage(msg):
    SECURITY_DATA = blpapi.Name("securityData")
    SECURITY = blpapi.Name("security")
    FIELD_DATA = blpapi.Name("fieldData")
    FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
    FIELD_ID = blpapi.Name("fieldId")
    ERROR_INFO = blpapi.Name("errorInfo")

    securityDataArray = msg.getElement(SECURITY_DATA)
    for securityData in securityDataArray.values():
        print(securityData.getElementAsString(SECURITY))
        fieldData = securityData.getElement(FIELD_DATA)
        for field in fieldData.elements():
            for i, row in enumerate(field.values()):
                for j in range(row.numElements()):
                    e = row.getElement(j)
                    print("Row %d col %d: %s %s" % (i, j, e.name(), e.getValue()))
                    

class IntradayBloombergTool:
    def __init__(self):
        self.in_load=Pybbg()
    def get_intraday_history__legacy(self,ticker, 
                             field_name, 
                             interval, 
                             startDateTime,
                             endDateTime):
        '''
        gets the historical values for an interval in Bloomberg 

        EXAMPLE
        ticker = 'eusa2 curncy', field_name='close', interval=1, startDateTime=datetime(2022,11,1,1,30),
        endDateTime=datetime.now()+timedelta(1)'''

        fld_list = [field_name]
        op_df= self.in_load.bdib(ticker, fld_list, 
                            startDateTime=startDateTime, 
                            endDateTime=endDateTime, 
                     eventType='TRADE', 
                            interval = interval)
        op_df.index.name='date'
        op_df.columns=['value']
        op_df['field_name']=field_name
        op_df['ticker']=ticker
        op_df.index=op_df.index.tz_localize('UTC').tz_convert('US/Eastern')
        op_df['timezone']='EST'
        op_df=op_df.reset_index()
        op_df['unique_identifier']= op_df['date'].astype(str)+'___'+op_df['field_name'].astype(str)+ '___'+op_df['ticker'].astype(str)
        return op_df


    def get_intraday_history(self, ticker, field_name, interval, startDateTime, endDateTime):
        """
        Fetch intraday historical data from Bloomberg for a given ticker.

        Args:
            ticker (str): Bloomberg ticker symbol.
            field_name (str): Field to fetch (e.g., 'close').
            interval (int): Time interval in minutes.
            startDateTime (datetime): Start of data period.
            endDateTime (datetime): End of data period.

        Returns:
            pd.DataFrame: Historical data with all fields, or an empty DataFrame if fetch fails.
        """
        fld_list = [field_name]
        try:
            # Retrieve intraday bar data
            op_df = self.in_load.bdib(
                ticker,
                fld_list,
                startDateTime=startDateTime,
                endDateTime=endDateTime,
                eventType='TRADE',
                interval=interval
            )
            
            # Preserve all necessary columns
            op_df.index.name = 'date'
            op_df.columns = ['value']
            op_df['field_name'] = field_name
            op_df['ticker'] = ticker
            op_df.index = op_df.index.tz_localize('UTC').tz_convert('US/Eastern')
            op_df['timezone'] = 'EST'
            op_df = op_df.reset_index()
            
            # Create unique identifier
            op_df['unique_identifier'] = (
                op_df['date'].astype(str) + '___' + 
                op_df['field_name'].astype(str) + '___' + 
                op_df['ticker'].astype(str)
            )
            
            return op_df

        except InvalidArgumentException as e:
            logging.warning(f"No data available for ticker: {ticker}. Returning empty DataFrame. Error: {e}")
            return pd.DataFrame(columns=['date', 'value', 'field_name', 'ticker', 'timezone', 'unique_identifier'])

        except Exception as e:
            logging.error(f"Unexpected error for ticker: {ticker}. Returning empty DataFrame. Error: {e}")
            return pd.DataFrame(columns=['date', 'value', 'field_name', 'ticker', 'timezone', 'unique_identifier'])


    def get_intraday_history_for_ticker_list(self, tickers, field_name, interval, startDateTime, endDateTime):
        """
        Fetch intraday historical data for a list of tickers from Bloomberg.

        Args:
            tickers (list): List of Bloomberg ticker symbols.
            field_name (str): Field to fetch (e.g., 'close').
            interval (int): Time interval in minutes.
            startDateTime (datetime): Start of data period.
            endDateTime (datetime): End of data period.

        Returns:
            dict: Dictionary of tickers and their corresponding DataFrames.
                Failed tickers will not appear in the result.
        """
        results = {}
        failed_tickers = []

        for ticker in tickers:
            try:
                logging.info(f"Fetching data for ticker: {ticker}")
                data = self.get_intraday_history(
                    ticker=ticker,
                    field_name=field_name,
                    interval=interval,
                    startDateTime=startDateTime,
                    endDateTime=endDateTime
                )
                if data is not None:
                    results[ticker] = data
                else:
                    failed_tickers.append(ticker)

            except Exception as e:
                logging.error(f"Unexpected error for ticker: {ticker}. Error: {e}")
                failed_tickers.append(ticker)

        # Log any failures
        if failed_tickers:
            logging.warning(f"Failed to fetch data for the following tickers: {', '.join(failed_tickers)}")

        return results



                