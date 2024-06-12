import blpapi
import pandas as pd
import datetime
import numpy as np
import json
import sqlalchemy
class BloombergDailyDataTool:
    """
    Mirrors the functions of Bloomberg Excel Tools but with
    special string casing for override caching into a database
    The tool will work without a bloomberg connection but only 
    with the functions: 
    get_recent_bdp_df
    get_recent_bdh_df
    get_recent_bds_df

    EXAMPLES

    BDH
    bloomberg_daily_tool.BDH(bbgTicker='msft us equity',
                                 field='px_last',
        startDate='2001-01-01',
        endDate='2002-01-01',
        periodicity='DAILY',
        overrides={})
    BDP
    bloomberg_daily_tool.BDP(bbgTickers=['msft us equity',
                                       'aapl us equity'],
                                    field='px_last',overrides={})

    BDS                                
    bloomberg_daily_tool.BDS(bbgTicker = '9984 jp equity', 
    field='eqy_dvd_hist_splits')

    """
    def __init__(self,pw_map, bloomberg_connection=False):
        def InitializeBBG():
            '''
            Initializes a BBG session and returns the session object

            Returns:
                BLPAPI BBG Session
            '''
            options = blpapi.SessionOptions()
            options.setServerHost('localhost')
            options.setServerPort(8194)
            session = blpapi.Session(options)
            session.start()
            # Open service to get historical data from
            if not session.openService("//blp/refdata"):
                print("Failed to open //blp/refdata")
            return session
        self.pw_map=pw_map
        self.session = ''
        if bloomberg_connection == True:
            self.session = InitializeBBG()
        
        
    def ProcessBDH(self, msg, request):
        '''
        Processes incoming event messages from the BLPAPI interface
        and returns a Pandas dataframe object with the assumption
        the date is the index

        Args:
            msg: BBG event message
            request: request object sent to the BBG session
        Returns:
            None if no data was recieved
            Pandas DataFrame if query was successful
        '''
        #fieldData, in the BBG Message Object, contains the requested data from BBG 
        secData = msg.getElement("securityData")
        fieldDataArray = secData.getElement("fieldData")

        if(fieldDataArray.numValues() == 0):
           # print "NO DATA RECEIVED"
            return None

        #Generate list of bbg fields that were requested and since BDH
        #function, manually add date column for DataFrame column names
        bbgReqFields = request.getElement("fields")
        bbgFields = ['date']
        for i in range(bbgReqFields.numValues()):
            bbgFields.append(bbgReqFields.getValueAsString(index=i))

        #data will store row data for the dataframe
        data = []  

        for i in range(fieldDataArray.numValues()):
            fieldData = fieldDataArray.getValueAsElement(i)
            tmpData = []

            #Explicitly get the element value from fieldData by passing the header (acts similar to
            #a key in a dictionary)
            for header in bbgFields:
                try:
                    #Even though we naively assume all data returned is a string, the pandas object
                    #will attempt to convert datatypes and so will the MySQL database
                    tmpData.append(fieldData.getElementAsString(header))  
                except:
                    #missing data, pass a Null to the dataframe
                    #This is more common as we go further back historically
                    #for example, PX_HIGH may exist but PX_LOW will not. This is
                    #a BBG-side issue.
                  #  print "Not all fields available"
                    tmpData.append(None)

            data.append(tmpData)

        dF = pd.DataFrame(data=data, columns=bbgFields)
        dF = dF.set_index("date")
        #dtIndex = pd.DatetimeIndex(dF.index)
        #dF = dF.reindex(dtIndex)
        #dF.index = dtIndex

        dF.index=[str(i).split('+')[0] for i in dF.index]
        dF.index = pd.to_datetime(dF.index)
        dF.index.name = 'date'
        dF.columns=['value']
        return dF
    def BDH(self, bbgTicker, field, startDate, endDate, periodicity = 'DAILY', overrides=None):
        '''
        Requires BBG specific information and returns the data to the provided callback function
        Args:
            bbgTicker = a string that represent bloomberg tickers
            fields = a collection of strings with bbg specific fields (e.g. ['PX_OPEN,'PX_LAST']
            startDate: a datetime object for the startDate or a string in YYYY-MM-DD format
            endDate: a datetime object for the endDate or a string in YYYY-MM-DD format
            overrides: (optional) A Dictionary of override fields, where the key is the override field, and 
                        value related to the BBG field value. E.g. BDH("spx index",fields="best eps",
                                                                    overrides={'BEST_FPERIOD_OVERRIDE':'1fq'})
        Returns:
            The output from the callback functions
        '''
        fields = field
        field_str =str(fields)
        
        if(startDate > endDate):
            print("Start date needs be before the end date")
            return

        session = self.session

        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")

        # Create and fill the request for the historical data
        request = refDataService.createRequest("HistoricalDataRequest")

        request.getElement("securities").appendValue(bbgTicker)

        #Convert single field to list of fields
        if(type(fields) == str):
            fields = [fields]
        #Append the list of fields
        for field in fields:
            request.getElement("fields").appendValue(field)

        #Configure overrides if passed
        if(overrides is not None):
            overrideBBGCollection = request.getElement("overrides")
            for overrideField in list(overrides.keys()):
                overrideObj = overrideBBGCollection.appendElement()
                overrideObj.setElement("fieldId", overrideField)
                overrideObj.setElement("value", overrides[overrideField])

        if(type(startDate) == str):
            startDate = datetime.datetime.strptime(startDate, "%Y-%m-%d")
        if(type(endDate) == str):
            endDate = datetime.datetime.strptime(endDate, "%Y-%m-%d")

        #Set periodicity, will be daily values by default
        request.set("periodicityAdjustment", "ACTUAL")
        request.set("periodicitySelection", periodicity)
        request.set("startDate", startDate.strftime("%Y%m%d"))
        request.set("endDate", endDate.strftime("%Y%m%d"))

        cid = self.session.sendRequest(request)
        try:
            # Process received events
            while(True):
                # We provide timeout to give the chance to Ctrl+C handling:
                ev = session.nextEvent(500)
                for msg in ev:
                    if cid in msg.correlationIds():
                        op_df = self.ProcessBDH(msg, request)
                        op_df['bbgTicker']=bbgTicker
                        op_df['overrides']= json.dumps(overrides)
                        op_df['field']=field_str
                        #op_df['datestring']=op_df.index.astype(str)
                        op_df['update_code']= (op_df['bbgTicker']
                                               +'__'+op_df['field']+'__'
                                               +op_df['overrides']
                                               )
                        
                        return op_df
                # Response completely received, so we could exit
                if(ev.eventType() == blpapi.Event.RESPONSE):
                    break
        except:
            pass
    def ProcessBDP(self, msg, request):
        """
        Processes incoming event messages from the BLPAPI interface
        and returns a dictionary object or list of dictionaries

        Args:
            msg: BBG event message
            request: request object sent to the BBG session
        Returns:
            Dictionary containing fields in BDP request as well as
            the bbgTicker value, assigned to key "bbgTicker" if only one
            security found.

            If multiple securities requested, returns a list of dictionaries,
            with the same data fields as described above
        """
        securityDataArray = msg.getElement("securityData")
        #data will be a dictionary of dictionaries where the key
        #represents the bbgCode (bbgTicker), and the value is a
        #dictionary of field Data
        data = {}
        #Go through each point in security data and then fieldData
        for securityData in list(securityDataArray.values()):
            #tmpFieldData will store the field data received from BBG
            tmpFieldData = {}
            bbgCode = securityData.getElementAsString("security").lower()
            fieldData = securityData.getElement("fieldData")

            for field in fieldData.elements():
                #If field is valid, set field name as key and
                #value as value
                if field.isValid():
                    tmpFieldData[str(field.name())] = field.getValueAsString()

            data[bbgCode] = tmpFieldData
        return data
    def BDP(self, bbgTickers, field, overrides={}):
        

        """
        Requires BBG specific information and returns the data to the provided callback function
        Args:
            bbgTickers = a string that represent bloomberg tickers, or a list of strings representing
            bbg tickers
            field = a bbg field (e.g. ['PX_OPEN,'PX_LAST']
            overrides: (optional) A Dictionary of override fields, where the key is the override field, and 
                        value related to the BBG field value. E.g. BDH("spx index",fields="best eps",
                        overrides={'REFERENCE_DATE':'20121031'})
                        This function must take two arguments:
                        a bloomberg message argument and the list of fields queried
        Returns:
            The output from the callback functions
        """
        bbgCodes =bbgTickers
        fields = field 
        field_name = str(field)
        
        cbFunction = self.ProcessBDP
        session = self.session

        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")

        # Create and fill the request for the reference data
        request = refDataService.createRequest("ReferenceDataRequest")

        if(type(bbgCodes) == str):
            bbgCodes = [bbgCodes]

        for ticker in bbgCodes:
            request.getElement("securities").appendValue(ticker)

        #Configure overrides if passed
        if(overrides is not None):
            overrideBBGCollection = request.getElement("overrides")
            for overrideField in list(overrides.keys()):
                overrideObj = overrideBBGCollection.appendElement()
                overrideObj.setElement("fieldId", overrideField)
                overrideObj.setElement("value", overrides[overrideField])

        #Convert single field to list of fields
        if(type(fields) == str):
            fields = [fields]
        #Append the list of fields
        for field in fields:
            request.getElement("fields").appendValue(field)

        cid = session.sendRequest(request)
        data = {}
        bdp_df = {}
        try:
            # Process received events
            while(True):
                # We provide timeout to give the chance to Ctrl+C handling:
                ev = session.nextEvent(500)
                for msg in ev:
                    if cid in msg.correlationIds():
                        data.update(cbFunction(msg, request))
                # Response completely received, so we could exit
                if(ev.eventType() == blpapi.Event.RESPONSE):
                    break
            bdp_df = data
            try:
                bdp_df = pd.DataFrame(data).transpose()
                bdp_df.index.name = 'bbgTicker'
                bdp_df.columns=['value']
                bdp_df['field']=field_name
                bdp_df['overrides']=json.dumps(overrides)
                bdp_df['pull_date']=gset.datetime_current_EST()
                bdp_df['update_code']= bdp_df.index +'__'+bdp_df['field']+'__'+bdp_df['overrides']
            except:
                pass
        except:
                
            pass
        
        
        return bdp_df
    
    def ProcessBDS(self, msg, request):
        """
        Processes incoming event messages from the BLPAPI interface
        and returns a dictionary object or list of dictionaries

        Args:
            msg: BBG event message
            request: request object sent to the BBG session
        Returns:
            DataFrame where columns are listed
        """
        #Get list of bbq fields we requested using the request object
        bbgReqFields = request.getElement("fields")
        bbgFields = []
        for i in range(bbgReqFields.numValues()):
            bbgFields.append(bbgReqFields.getValueAsString(index=i))


        allData = msg.getElement("securityData")
        #allDataDF = pd.DataFrame()
        for secData in list(allData.values()):
            #fieldData, in the BBG Message Object, contains the requested data from BBG
            fieldData = secData.getElement("fieldData")

            #Cycle through all the bbgFields requested and obtain the fieldData. For BDS
            #the field Data will be a dictionary-type object, but in a weird BBG response object
            #which is difficult and annoying to parse. We may be able to print the string and 
            #load it into a JSON object.
            for bbgField in bbgFields:
                data = fieldData.getElement(bbgField)
                columns = []

                #a BDS function will contain it's own column headers. Find these out
                #by looking at the first element (index 0) element that returned and pulling
                #out the header (bdsCol.name().__str__())
                for bdsCol in data.getValue(0).elements():
                    columns.append(bdsCol.name().__str__())

                #With columns figured out, create the DataFrame using an int as the index
                dataDF = pd.DataFrame(index= np.arange(0, int(data.numValues())),columns = columns)

                #Go through the values (think of them as rows) and then the Elements (think fo them as columns)
                #Then use wordy BBG api commands to get info
                for rowIndex in range(0,data.numValues()):
                    bdsData = data.getValue(rowIndex)
                    for colIndex in range(0,bdsData.numElements()):
                        bdsDataNew = bdsData.getElement(colIndex)
                        try:
                            dataDF[bdsDataNew.name().__str__()].loc[rowIndex] = bdsDataNew.getValueAsString()
                        except:
                            dataDF[bdsDataNew.name().__str__()].loc[rowIndex] = np.nan
                            pass
        return dataDF
    ''' 
    def BDS(self, bbgTicker, field, overrides={}):
        """
            Requires BBG specific information and returns the data to the provided callback function
        Args:
            bbgCode = a string representing a BBG ticker (only single bbgCodes supported now)
            fields = a collection of strings with bbg specific fields 
            overrides: (optional) A Dictionary of override fields, where the key is the override field, and 
                        value related to the BBG field value. E.g. BDH("spx index",fields="best eps",
                                                                    overrides={'BEST_FPERIOD_OVERRIDE':'1fq'})
            cbFunction: (optional) a function to process message events. This function must take two arguments:
                         a bloomberg message argument and the list of fields queried

        Returns:
            The output from the callback functions
        """
        fields = field
        field_name= str(fields)
        
        bbgCode=bbgTicker
        cbFunction=self.ProcessBDS
        session = self.session

        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")

        # Create and fill the request for the reference data
        request = refDataService.createRequest("ReferenceDataRequest")

        #No support for multiple securities yet
        #if(type(bbgCodes) == str):
        #    bbgCodes = [bbgCodes]

        #for ticker in bbgCodes:
        request.getElement("securities").appendValue(bbgCode)

        #Convert single field to list of fields
        if(type(fields) == str):
            fields = [fields]
        #Append the list of fields
        for field in fields:
            request.getElement("fields").appendValue(field)


        #Configure overrides if passed
        if(overrides is not None):
            overrideBBGCollection = request.getElement("overrides")
            for overrideField in list(overrides.keys()):
                overrideObj = overrideBBGCollection.appendElement()
                overrideObj.setElement("fieldId", overrideField)
                overrideObj.setElement("value", overrides[overrideField])

        cid = session.sendRequest(request)

        try:
            # Process received events
            while(True):
                # We provide timeout to give the chance to Ctrl+C handling:
                ev = session.nextEvent(500)
                for msg in ev:
                    if cid in msg.correlationIds():
                        #return cbFunction(msg, request)
                        xbds=cbFunction(msg, request)
                        #xbds = bloomberg_daily_tool.BDS('9984 jp equity',"EQY_DVD_HIST_SPLITS")
                        xbds['bbgTicker']=bbgTicker
                        restacked= pd.DataFrame(xbds.set_index('bbgTicker').stack()).reset_index()
                        restacked.columns=['bbgTicker','subfield','value']
                        restacked['overrides']=json.dumps(overrides)
                        restacked['field']=field
                        restacked['pull_date'] = gset.datetime_current_EST()
                        restacked['update_code'] = restacked['bbgTicker']+'__'+restacked['field']+'__'+restacked['overrides']
                        return restacked
                # Response completely received, so we could exit
                if(ev.eventType() == blpapi.Event.RESPONSE):
                    break
        except:
            pass
        
    ''' 

    def BDS(self, bbgTicker, field, overrides={}):
        """
            Requires BBG specific information and returns the data to the provided callback function
        Args:
            bbgCode = a string representing a BBG ticker (only single bbgCodes supported now)
            fields = a collection of strings with bbg specific fields 
            overrides: (optional) A Dictionary of override fields, where the key is the override field, and 
                        value related to the BBG field value. E.g. BDH("spx index",fields="best eps",
                                                                    overrides={'BEST_FPERIOD_OVERRIDE':'1fq'})
            cbFunction: (optional) a function to process message events. This function must take two arguments:
                         a bloomberg message argument and the list of fields queried

        Returns:
            The output from the callback functions
        """
        fields = field
        field_name= str(fields)
        
        bbgCode=bbgTicker
        cbFunction=self.ProcessBDS
        session = self.session

        # Obtain previously opened service
        refDataService = session.getService("//blp/refdata")

        # Create and fill the request for the reference data
        request = refDataService.createRequest("ReferenceDataRequest")

        #No support for multiple securities yet
        #if(type(bbgCodes) == str):
        #    bbgCodes = [bbgCodes]

        #for ticker in bbgCodes:
        request.getElement("securities").appendValue(bbgCode)

        #Convert single field to list of fields
        if(type(fields) == str):
            fields = [fields]
        #Append the list of fields
        for field in fields:
            request.getElement("fields").appendValue(field)


        #Configure overrides if passed
        if(overrides is not None):
            overrideBBGCollection = request.getElement("overrides")
            for overrideField in list(overrides.keys()):
                overrideObj = overrideBBGCollection.appendElement()
                overrideObj.setElement("fieldId", overrideField)
                overrideObj.setElement("value", overrides[overrideField])

        cid = session.sendRequest(request)

        try:
            # Process received events
            while(True):
                # We provide timeout to give the chance to Ctrl+C handling:
                ev = session.nextEvent(500)
                for msg in ev:
                    if cid in msg.correlationIds():
                        #return cbFunction(msg, request)
                        xbds=cbFunction(msg, request)
                        #xbds = bloomberg_daily_tool.BDS('9984 jp equity',"EQY_DVD_HIST_SPLITS")
                        xbds['bbgTicker']=bbgTicker
                        restacked= pd.DataFrame(xbds.set_index('bbgTicker').stack()).reset_index()
                        restacked.columns=['bbgTicker','subfield','value']
                        restacked['overrides']=json.dumps(overrides)
                        restacked['field']=field
                        restacked['pull_date'] = datetime.datetime.now()
                        restacked['update_code'] = restacked['bbgTicker']+'__'+restacked['field']+'__'+restacked['overrides']
                        return restacked
                # Response completely received, so we could exit
                if(ev.eventType() == blpapi.Event.RESPONSE):
                    break
        except:
            pass
