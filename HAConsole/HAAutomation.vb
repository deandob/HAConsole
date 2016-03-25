Imports System.Data.SQLite
Imports System.IO
Imports Commons

Public Class Automation

    Private Shared AutoConn As SQLite.SQLiteConnection
    Private Shared AccessDB As New Object                               ' Synclock object for accessing Datatable and SQLite to ensure access is threadsafe
    Private Shared TransformLock As New Object

    Public Shared ActionsDT As New DataTable                            ' Datatable for actions
    Private Shared ActionsDA As New SQLite.SQLiteDataAdapter
    Private Shared ActionsSQLCmd As SQLite.SQLiteCommand                ' Command for insert SQL Actions
    Public Shared EventActionsDT As New DataTable                       ' Datatable for event actions
    Private Shared EventActionsDA As New SQLite.SQLiteDataAdapter
    Private Shared EventActionsSQLCmd As SQLite.SQLiteCommand           ' Command for insert SQL Event actions
    Public Shared EventsDT As New DataTable                             ' Datatable for events
    Private Shared EventsDA As New SQLite.SQLiteDataAdapter
    Private Shared EventsSQLCmd As SQLite.SQLiteCommand                 ' Command for insert SQL Events
    Public Shared EventTriggersDT As New DataTable                      ' Datatable for Event triggers
    Private Shared EventTriggersDA As New SQLite.SQLiteDataAdapter
    Private Shared EventTriggersSQLCmd As SQLite.SQLiteCommand          ' Command for insert SQL Event triggers
    Public Shared TriggersDT As New DataTable                           ' Datatable for triggers
    Private Shared TriggersDA As New SQLite.SQLiteDataAdapter
    Private Shared TriggersSQLCmd As SQLite.SQLiteCommand               ' Command for insert SQL Triggers

    Public Shared TransFuncsDT As New DataTable                           ' Datatable for triggers
    Private Shared TransFuncsDA As New SQLite.SQLiteDataAdapter
    Private Shared TransFuncsSQLCmd As SQLite.SQLiteCommand               ' Command for insert SQL Triggers
    Public Shared FuncsDT As New DataTable                           ' Datatable for triggers
    Private Shared FuncsDA As New SQLite.SQLiteDataAdapter
    Private Shared FuncsSQLCmd As SQLite.SQLiteCommand               ' Command for insert SQL Triggers

    Private Shared AutoFile As String                                   ' Location for the automation DB file

    ' NOTE: All time/dates are in UTC

    'TODO: Dynamic update to the trigger and transform engines when changes to the DB are made. Currently have to reset to lock in a change.

#Region "Initialization"
    ' Initialize the automation database including creating the tables if the db file doesn't exist
    Public Shared Sub InitAutoDB()
        WriteConsole(True, "Starting Event Management...")
        AutoFile = HS.Ini.Get("database", "AutomationName", "automation")
        AutoConn = New SQLite.SQLiteConnection
        AutoConn.ConnectionString = "Data Source=" + HS.DBLocn + "\" + AutoFile + HAServices.HomeServices.LOG_EXT + ";"c
        If Not File.Exists(HS.DBLocn + "\"c + AutoFile + HAServices.HomeServices.LOG_EXT) Then                    ' If no log file exists then create it
            AutoConn.Open()                                              ' Creates datafile if one does not exist (so has to execute after we check for data file existence)
            Dim SQLCmd As SQLite.SQLiteCommand = AutoConn.CreateCommand
            If Environment.OSVersion.Platform = PlatformID.Win32NT Then             ' Tune the file size for the operating system (can set this in the ini file)
                SQLCmd.CommandText = "PRAGMA page_size=" + HS.Ini.Get("System", "WindowsClusterSize", "4096") + ";"c          ' Windows
            Else
                SQLCmd.CommandText = "PRAGMA page_size=" + HS.Ini.Get("System", "LinuxClusterSize", "1024") + ";"c          ' Linux
            End If
            SQLCmd.ExecuteNonQuery()
            SQLCmd.CommandText = "CREATE TABLE Actions (ActionName TEXT PRIMARY KEY, ActionDescription TEXT, ActionScript TEXT, ActionScriptParam TEXT, ActionDelay INTEGER, ActionRandom BOOLEAN, ActionFunction INTEGER, ActionLogLevel INTEGER, ActionNetwork INTEGER, ActionCategory INTEGER, ActionClass TEXT, ActionInstance TEXT, ActionScope TEXT, ActionData TEXT);"
            SQLCmd.ExecuteNonQuery()
            SQLCmd.CommandText = "CREATE TABLE EventActions (ID INTEGER PRIMARY KEY, EventName TEXT, ActionName TEXT);"
            SQLCmd.ExecuteNonQuery()
            SQLCmd.CommandText = "CREATE TABLE Events (EventName TEXT PRIMARY KEY, EventDescription TEXT, EventActive BOOLEAN, EventNumRecur INTEGER, EventLastFired INTEGER, EventOneOff BOOLEAN, EventStart INTEGER, EventStop INTEGER);"
            SQLCmd.ExecuteNonQuery()
            SQLCmd.CommandText = "CREATE TABLE EventTriggers (ID INTEGER PRIMARY KEY, EventName TEXT, TrigName TEXT);"
            SQLCmd.ExecuteNonQuery()
            SQLCmd.CommandText = "CREATE TABLE Triggers (TrigName TEXT PRIMARY KEY, TrigDescription TEXT, TrigScript TEXT, TrigScriptParam TEXT, TrigScriptData TEXT, TrigScriptCond INTEGER, TrigStateNetwork INTEGER, TrigStateCategory INTEGER, TrigStateClass TEXT, TrigStateInstance TEXT, TrigStateScope TEXT, TrigStateCond TEXT, TrigStateData TEXT, TrigChgNetwork INTEGER, TrigChgCategory INTEGER, TrigChgClass TEXT, TrigChgInstance TEXT, TrigChgScope TEXT, TrigChgCond TEXT, TrigChgData TEXT, "
            SQLCmd.CommandText = SQLCmd.CommandText + "TrigDateFrom INTEGER, TrigTimeFrom INTEGER, TrigDateTo INTEGER, TrigTimeTo INTEGER,TrigFortnightly BOOLEAN, TrigMonthly BOOLEAN, TrigYearly BOOLEAN, TrigSunrise BOOLEAN, TrigSunset BOOLEAN, TrigDayTime BOOLEAN, TrigNightTime BOOLEAN, "
            SQLCmd.CommandText = SQLCmd.CommandText + "TrigMon BOOLEAN, TrigTue BOOLEAN, TrigWed BOOLEAN, TrigThu BOOLEAN, TrigFri BOOLEAN, TrigSat BOOLEAN, TrigSun BOOLEAN, TrigActive BOOLEAN, TrigInactive BOOLEAN, TrigTimeofDay INTEGER, TrigLastFired INTEGER);"
            SQLCmd.ExecuteNonQuery()

            SQLCmd.CommandText = "CREATE TABLE TransFuncs (TFName TEXT PRIMARY KEY, TFDescription TEXT, Enabled BOOLEAN, RoundDec INTEGER, Network INTEGER, Category INTEGER, Class TEXT, Instance TEXT, Scope TEXT);"
            SQLCmd.ExecuteNonQuery()
            SQLCmd.CommandText = "CREATE TABLE Funcs (FuncIndex INTEGER PRIMARY KEY, TransFunc TEXT, FuncType TEXT, Val REAL, Network INTEGER, Category INTEGER, Class TEXT, Instance TEXT, Scope TEXT, History TEXT, Type TEXT, Prev INTEGER, PrevUnit TEXT, PrevFromDate INTEGER, PrevFromTime INTEGER);"
            SQLCmd.ExecuteNonQuery()

            AutoConn.Close()
            HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "FILE", "CREATED", "New Automation file created", "SYSTEM")
        End If

        AutoConn.Open()                                                         ' Open the database

        'Dim SQLCmd1 As SQLite.SQLiteCommand = AutoConn.CreateCommand
        'SQLCmd1.CommandText = "CREATE TABLE TransFuncs (TFName TEXT PRIMARY KEY, TFDescription TEXT, Enabled BOOLEAN, RoundDec INTEGER, Network INTEGER, Category INTEGER, Class TEXT, Instance TEXT, Scope TEXT);"
        'SQLCmd1.ExecuteNonQuery()
        'SQLCmd1.CommandText = "CREATE TABLE Funcs (FuncIndex INTEGER PRIMARY KEY, TransFunc TEXT,FuncType TEXT, Val REAL, Network INTEGER, Category INTEGER, Class TEXT, Instance TEXT, Scope TEXT, History TEXT, Type TEXT, Prev INTEGER, PrevUnit TEXT, PrevFromDate INTEGER, PrevFromTime INTEGER);"
        'SQLCmd1.ExecuteNonQuery()
        'AutoConn.Close()
        'Return

        ' Create the SQL command objects
        ActionsSQLCmd = AutoConn.CreateCommand
        EventsSQLCmd = AutoConn.CreateCommand
        EventActionsSQLCmd = AutoConn.CreateCommand
        EventTriggersSQLCmd = AutoConn.CreateCommand
        TriggersSQLCmd = AutoConn.CreateCommand

        TransFuncsSQLCmd = AutoConn.CreateCommand
        FuncsSQLCmd = AutoConn.CreateCommand

        ' Load the datatables
        ActionsSQLCmd.CommandText = "SELECT * FROM Actions"
        ActionsDA.SelectCommand = ActionsSQLCmd
        ActionsDA.Fill(ActionsDT)
        EventsSQLCmd.CommandText = "SELECT * FROM Events"
        EventsDA.SelectCommand = EventsSQLCmd
        EventsDA.Fill(EventsDT)
        EventActionsSQLCmd.CommandText = "SELECT * FROM EventActions"
        EventActionsDA.SelectCommand = EventActionsSQLCmd
        EventActionsDA.Fill(EventActionsDT)
        EventTriggersSQLCmd.CommandText = "SELECT * FROM EventTriggers"
        EventTriggersDA.SelectCommand = EventTriggersSQLCmd
        EventTriggersDA.Fill(EventTriggersDT)
        TriggersSQLCmd.CommandText = "SELECT * FROM Triggers"
        TriggersDA.SelectCommand = TriggersSQLCmd
        TriggersDA.Fill(TriggersDT)

        TransFuncsSQLCmd.CommandText = "SELECT * FROM TransFuncs"
        TransFuncsDA.SelectCommand = TransFuncsSQLCmd
        TransFuncsDA.Fill(TransFuncsDT)
        FuncsSQLCmd.CommandText = "SELECT * FROM Funcs"
        FuncsDA.SelectCommand = FuncsSQLCmd
        FuncsDA.Fill(FuncsDT)

        LoadTransformChs()                                            ' Load transforms channels and history

    End Sub
#End Region
    '#####################################################################################################################################################################################
#Region "Helper Functions"
    ' Extract Event information from the datatable based on filter
    Public Shared Function GetEventsInfo(EventName As String) As DataRow()
        If EventName <> "" Then
            Return EventsDT.Select("EventName = '" + EventName + "'")
        Else
            Return EventsDT.Select()
        End If
    End Function

    ' Extract Action information from the datatable based on filter
    Public Shared Function GetActionsInfo(ActionName As String) As DataRow()
        If ActionName <> "" Then
            Return ActionsDT.Select("ActionName = '" + ActionName + "'")
        Else
            Return ActionsDT.Select()
        End If
    End Function

    ' Extract Trigger information from the datatable based on filter
    Public Shared Function GetTriggersInfo(TrigName As String) As DataRow()
        If TrigName <> "" Then
            Return TriggersDT.Select("TrigName = '" + TrigName + "'")
        Else
            Return TriggersDT.Select()
        End If
    End Function

    ' Extract EventActions information from the datatable based on filter
    Public Shared Function GetEventActionsInfo(EventName As String) As DataRow()
        If EventName <> "" Then
            Return EventActionsDT.Select("EventName = '" + EventName + "'")
        Else
            Return EventActionsDT.Select()
        End If
    End Function

    ' Extract EventTrigger information from the datatable based on filter
    Public Shared Function GetEventTriggersInfo(EventName As String) As DataRow()
        If EventName <> "" Then
            Return EventTriggersDT.Select("EventName = '" + EventName + "'")
        Else
            Return EventTriggersDT.Select()
        End If
    End Function

    ' Extract Transform information from the datatable based on filter
    Public Shared Function GetTransformsInfo(TransformName As String) As DataRow()
        If TransformName <> "" Then
            Return TransFuncsDT.Select("TFName = '" + TransformName + "'")
        Else
            Return TransFuncsDT.Select()
        End If
    End Function

    ' Extract Function information from the datatable based on filter
    Public Shared Function GetFunctionsInfo(TransformName As String) As DataRow()
        If TransformName <> "" Then
            Return FuncsDT.Select("TransFunc = '" + TransformName + "'")
        Else
            Return FuncsDT.Select()
        End If
    End Function

    ' Return the names of the records in the appropriate table
    Public Shared Function GetNames(TableName As String) As DataRow()
        Select Case TableName.ToUpper
            Case Is = "ACTIONS"
                Return ActionsDT.DefaultView.ToTable("ActionNames", False, "ActionName").Select()
            Case Is = "TRIGGERS"
                Return TriggersDT.DefaultView.ToTable("TriggerNames", False, "TrigName").Select()
            Case Is = "EVENTS"
                Return EventsDT.DefaultView.ToTable("EventNames", False, "EventName").Select()
            Case Is = "TRIGGERSDESC"
                Return TriggersDT.DefaultView.ToTable("EventTriggers", False, {"TrigName", "TrigDescription"}).Select()
            Case Is = "ACTIONSDESC"
                Return ActionsDT.DefaultView.ToTable("ActionTriggersNames", False, {"ActionName", "ActionDescription"}).Select()
            Case Is = "TRANSFUNCS"
                Return TransFuncsDT.DefaultView.ToTable("TransFuncNames", False, {"TFName", "TFDescription"}).Select()
            Case Is = "WIDGETS", "SCRIPTS", "PLUGINS"
                Dim ProgramTable As New DataTable
                ProgramTable.Columns.Add("Name")
                ProgramTable.Columns.Add("Desc")
                Dim fileNames() As String = HS.GetProgramNames(TableName.ToUpper).Split(","c)
                For Each file In fileNames
                    Dim row As DataRow = ProgramTable.NewRow()
                    If file.IndexOf("_") = -1 Then
                        row("Name") = file.Substring(0, file.LastIndexOf("."))       ' strip extension
                    Else
                        row("Name") = file.Substring(0, file.IndexOf("_"))          ' strip description delimited by _
                        Dim TempStr As String = file.Substring(file.IndexOf("_") + 1)
                        row("Desc") = TempStr.Substring(0, TempStr.LastIndexOf("."))
                    End If
                    ProgramTable.Rows.Add(row)
                Next
                Return ProgramTable.Select()
        End Select
        Return Nothing
    End Function

#End Region
    '#####################################################################################################################################################################################
#Region "Event Processing"

    ' Create a new message based on applying a functional transform to existing message based on saved functions (executed in order saved)
    Public Shared Function Transform(EventMessage As Structures.HAMessageStruc) As Boolean
        ' Calculate sets when starting up and adjusting real time (eg. average power consumption). Each set starts from a defined time & spans to current time (as it is real time)
        ' Relative time: minute, hour, day, week, month, year (eg. 2014), either current or elapsed. Absolute time: Since <DATE><TIME>

        ' Loop through Func table looking for an event message match with minimal CPU usage (only matching records with a channel)
        SyncLock TransformLock
            WriteConsole(False, "Start checking transforms for " + EventMessage.Instance)
            Dim TestTrans = ""
            For Each TransRow As DataRow In FuncsDT.Rows
                'WriteConsole(True, "TRANSFUNC " + CStr(TransRow("TransFunc")) + " NETWORK" + CStr(EventMessage.Network) + "  CATEGORY " + CStr(TransRow("Category")) + " CLASS " + CStr(TransRow("Class")) + " ISTANCE " + CStr(TransRow("Instance")))
                Dim TransMessage As Structures.HAMessageStruc
                TransMessage.Network = CByte(TransRow("Network"))
                TransMessage.Category = HS.GetCatName(CByte(TransRow("Category")))
                TransMessage.ClassName = CStr(TransRow("Class"))
                TransMessage.Instance = CStr(TransRow("Instance"))
                TransMessage.Scope = CStr(TransRow("Scope"))
                If TransMessage.Instance <> "" And TransMessage.Instance <> "*" Then If TransMessage.Instance.ToUpper <> EventMessage.Instance.ToUpper Then Continue For
                If TransMessage.ClassName.ToUpper <> EventMessage.ClassName.ToUpper Then Continue For
                'If TransMessage.Scope <> "" Then If TransMessage.Scope.ToUpper <> EventMessage.Scope.ToUpper Then Continue For ' allow partial match
                If TransMessage.Scope <> "" Then If EventMessage.Scope.IndexOf(TransMessage.Scope, StringComparison.OrdinalIgnoreCase) = -1 Then Continue For ' allow partial match
                If TransMessage.Category <> EventMessage.Category Then Continue For
                If EventMessage.Network <> 0 Then If TransMessage.Network <> EventMessage.Network Then Continue For
                TestTrans = CStr(TransRow("TransFunc")).ToUpper                              ' Found a match on channel in one of the function records

                ' With a valid match, extract the transform record and all associated functions and process them
                If TestTrans <> "" Then          ' Process match
                    WriteConsole(False, "Transform: " + TestTrans)
                    WriteConsole(False, "-------- Start Transform: " + TestTrans)
                    Dim TFRow As DataRow() = TransFuncsDT.Select("TFName = '" + TestTrans.Trim() + "'"c)            ' Get the transform record
                    If CBool(TFRow(0)("Enabled")) Then
                        Dim FuncRows As DataRow() = FuncsDT.Select("TransFunc = '" + TestTrans.Trim() + "'", "FuncIndex ASC")       ' Get all functions for the transform
                        Dim CalcVal As Single = 0
                        For Each FuncRow As DataRow In FuncRows                             ' Process each function saved in transform
                            Dim myVal As Single = 0
                            Dim FuncMessage As Structures.HAMessageStruc
                            If IsDBNull(FuncRow("Val")) Then                                ' If Val field is null, process a function message
                                FuncMessage.Network = CByte(FuncRow("Network"))
                                FuncMessage.Category = HS.GetCatName(CByte(FuncRow("Category")))
                                FuncMessage.ClassName = CStr(FuncRow("Class"))
                                FuncMessage.Instance = CStr(FuncRow("Instance"))
                                FuncMessage.Scope = CStr(FuncRow("Scope"))
                                Dim Duration = "_" + CStr(FuncRow("PrevUnit")).ToUpper() + CStr(FuncRow("Prev"))

                                Dim HistFunc = CStr(FuncRow("History")).ToUpper()
                                Select Case HistFunc
                                    Case Is = "CURRENT"
                                        Select Case TransMessage.Instance
                                            Case Is = "*"                           ' Channel token of '*' Count all the non 0 values in the channel list
                                                FuncMessage.Instance = ""
                                                Dim getChs As Dictionary(Of Structures.StateStoreKey, String) = HS.SearchStoreStates(FuncMessage)
                                                For Each Ch In getChs
                                                    If CSng(Ch.Value) > 0 Then myVal = myVal + 1
                                                Next
                                            Case Is = "+"                           ' Channel token of '+' Add all the non 0 values in the channel list
                                            Case Is = ""
                                                Single.TryParse(HS.GetStoreState(FuncMessage), myVal)       ' Get message data from the state store and use as val
                                            Case Else
                                                Single.TryParse(HS.GetStoreState(FuncMessage), myVal)       ' Get message data from the state store and use as val
                                        End Select

                                    Case Is = "AVERAGE"
                                        Dim DeltaTime As Long = Date.UtcNow.Ticks - CLng(HS.GetStoreState(FuncMessage))
                                        FuncMessage.Scope = "_PREVVAL" + Duration
                                        Dim PrevVal As Single = CSng(HS.GetStoreState(FuncMessage))
                                        FuncMessage.Scope = "_SUMVAL" + Duration
                                        Dim SumVal As Double = CDbl(HS.GetStoreState(FuncMessage)) + PrevVal * DeltaTime           ' Calculate with previous value weighted by time delta between prev value time & now
                                        FuncMessage.Scope = "_SUMTIME" + Duration
                                        Dim sumTime As Double = CDbl(HS.GetStoreState(FuncMessage)) + DeltaTime

                                        ' Save current time and sums for next calculation
                                        FuncMessage.Scope = "_PREVTIME" + Duration
                                        FuncMessage.Data = CStr(Date.UtcNow.Ticks)
                                        HS.SetStoreState(FuncMessage)
                                        FuncMessage.Scope = "_PREVVAL" + Duration
                                        FuncMessage.Data = CStr(PrevVal)
                                        HS.SetStoreState(FuncMessage)
                                        FuncMessage.Scope = "_SUMVAL" + Duration
                                        FuncMessage.Data = CStr(SumVal)
                                        HS.SetStoreState(FuncMessage)
                                        FuncMessage.Scope = "_SUMTIME" + Duration
                                        FuncMessage.Data = CStr(SumTime)
                                        HS.SetStoreState(FuncMessage)

                                        If SumTime <> 0 Then myVal = CSng(SumVal / SumTime) Else myVal = 0 ' Weighted average

                                    Case Is = "SUM"

                                    Case Is = "COUNT"

                                    Case Is = "MAX", "MAXTIMENEWEST", "MAXTIMEOLDEST"
                                        FuncMessage.Scope = ""                          ' Check all values in the statestore for the channel
                                        Dim foundKeys As Dictionary(Of Structures.StateStoreKey, String) = HS.SearchStoreStates(FuncMessage)
                                        Dim testSng As Single
                                        For Each Key In foundKeys
                                            If Single.TryParse(Key.Value, testSng) And Key.Key.Scope = "_MAX" + Duration Then
                                                myVal = testSng                                         ' historical max value (by duration)
                                                If CSng(EventMessage.Data) > testSng Then               ' If current is larger than duration max, update statestore with value + time
                                                    FuncMessage.Scope = "_MAX" + Duration
                                                    FuncMessage.Data = EventMessage.Data
                                                    HS.SetStoreState(FuncMessage)
                                                    FuncMessage.Scope = "_MAXTIME" + Duration
                                                    FuncMessage.Data = HAUtils.ToJSTime(EventMessage.Time.Ticks()).ToString()
                                                    HS.SetStoreState(FuncMessage)
                                                    If HistFunc <> "MAX" Then myVal = EventMessage.Time.Ticks() Else myVal = CSng(EventMessage.Data)
                                                    Exit For
                                                End If
                                            End If
                                            If Key.Key.Scope = "_MAXTIME" + Duration And HistFunc <> "MAX" Then myVal = testSng : Exit For
                                        Next

                                    Case Is = "MIN", "MINTIMENEWEST", "MINTIMEOLDEST"
                                        FuncMessage.Scope = ""                          ' Check all values in the statestore for the channel
                                        Dim foundKeys As Dictionary(Of Structures.StateStoreKey, String) = HS.SearchStoreStates(FuncMessage)
                                        Dim testSng As Single
                                        For Each Key In foundKeys
                                            If Single.TryParse(Key.Value, testSng) And Key.Key.Scope = "_MIN" + Duration Then
                                                myVal = testSng                                         ' historical max value (by duration)
                                                If CSng(EventMessage.Data) < testSng Then               ' If current is larger than duration max, update statestore with value + time
                                                    FuncMessage.Scope = "_MIN" + Duration
                                                    FuncMessage.Data = EventMessage.Data
                                                    HS.SetStoreState(FuncMessage)
                                                    FuncMessage.Scope = "_MINTIME" + Duration
                                                    FuncMessage.Data = HAUtils.ToJSTime(EventMessage.Time.Ticks()).ToString()
                                                    HS.SetStoreState(FuncMessage)
                                                    If HistFunc <> "MIN" Then myVal = EventMessage.Time.Ticks() Else myVal = CSng(EventMessage.Data)
                                                    Exit For
                                                End If
                                            End If
                                            If Key.Key.Scope = "_MINTIME" + Duration And HistFunc <> "MIN" Then myVal = testSng : Exit For
                                        Next
                                End Select
                            Else
                                Single.TryParse(CStr(FuncRow("Val")), myVal)                ' Val is a number
                            End If
                            CalcVal = ApplyOper(CalcVal, CStr(FuncRow("FuncType")), myVal)  ' calculate value based on function
                            If IsNothing(CalcVal) Then
                                WriteConsole(True, "WARNING - Transform " + TestTrans + " function " + CStr(FuncRow("FuncType")) + " aborted due to error in operation " + CStr(FuncRow("FuncType")) + "(" + CStr(myVal) + ")")
                                Exit For
                            End If
                            WriteConsole(False, TestTrans + " function: " + CStr(FuncRow("FuncType")) + " Incremental value: " + CStr(CalcVal))
                        Next
                        WriteConsole(False, "-------- End Transform: " + TestTrans)

                        ' Apply rounding to the result, and create new event message
                        'WriteConsole(False, "Transform created message: " + HS.GetCatName(CByte(TFRow(0)("Category"))) + "/" + CStr(TFRow(0)("Class")) + " " + CStr(TFRow(0)("Instance")) + ":" + CStr(CalcVal) + " (" + CStr(TFRow(0)("Scope")) + ")")
                        HS.CreateMessage(CStr(TFRow(0)("Class")), HAConst.MessFunc.EVENT, HAConst.MessLog.NORMAL, CStr(TFRow(0)("Instance")), CStr(TFRow(0)("Scope")), _
                                         CStr(Math.Round(CalcVal, CInt(TFRow(0)("RoundDec")), MidpointRounding.AwayFromZero)), HS.GetCatName(CByte(TFRow(0)("Category"))), GlobalVars.myNetNum)
                    End If
                End If
            Next

        End SyncLock

    End Function

    ' Apply programmatically the relevant operator for the transform
    Private Shared Function ApplyOper(intermVal As Single, oper As String, val As Single) As Single
        Select Case oper.ToUpper
            Case Is = "START"
                Return val                          ' Initial value has no operator
            Case Is = "ADD"
                Return (intermVal + val)
            Case Is = "SUBTRACT"
                Return (intermVal - val)
            Case Is = "MULTIPLY"
                Return (intermVal * val)
            Case Is = "DIVIDE"
                If val <> 0 Then Return (intermVal / val)
            Case Is = "MAX"                       ' threshold setting maximum value (value can't be larger)
                Return (Math.Min(intermVal, val))
            Case Is = "MIN"                       ' threshold setting minimum value (value can't be smaller)
                Return (Math.Max(intermVal, val))
            Case Is = "SCRIPT"
                Return 0            ' TODO
        End Select
        Return Nothing
    End Function

    ' Setup transforms at startup and populate history data
    ' Transform: sets (count, sum, average, change, max, min, mean, sd) from when to when
    ' For set calculations, the database has stored all changed values in the timerange
    ' eg. at 7:21:33 the value changed from 10 to 12. Next record is the next change back to 10 at 7:21:54
    ' Q: How many times has the alarm been triggered today? -> select count alarmCh where alarmCh = 1
    ' Q: Which light has been on the longest? select sum all CBUS messages where data <> 0 between now and now - year

    ' Domains: Real time calculations including set calculations (which has a time dimension). Historical reporting which has a time dimension.
    ' Pulse data - only get a single pulse message with no state change (no state change). Either don't allow pulses or use a pulse IO type.

    'TODO: Call this when any transform is modified, else you need to restart server

    ' Add dynamic channels for all transforms
    Public Shared Function LoadTransformChs() As Boolean
        ' Setup transform dynamic channels
        Dim TransFuncRecs() As DataRow = GetTransformsInfo("")
        For Each TFrec As DataRow In TransFuncRecs
            Dim newCh As New Structures.ChannelStruc
            newCh.Name = CStr(TFrec("Instance"))
            newCh.Desc = CStr(TFrec("TFDescription"))
            newCh.IO = "output"
            newCh.Units = CStr(TFrec("Scope"))
            newCh.Type = "TRANSFORM"
            newCh.Value = ""
            newCh.Attribs = New List(Of Structures.ChannelAttribStruc)

            Dim TransPlug As New Structures.PlugStruc
            TransPlug.Desc = "Plugin created by transform"
            TransPlug.ClassName = CStr(TFrec("Class"))
            TransPlug.Category = HS.GetCatName(CByte(TFrec("Category")))
            TransPlug.Type = "TRANSFORM"
            TransPlug.Channels = New List(Of Structures.ChannelStruc)
            HS.addPlugin(TransPlug, False)                                  ' Add a plugin (needed for channel definition) even if temporary, don't overwrite real plugin
            HS.modChannel("ADD", HS.GetCatName(CByte(TFrec("Category"))), CStr(TFrec("Class")), newCh)
        Next

        ' Load history data for functions with history by loading from log database 
        ' TODO: Only load for active transforms? But then have to manage enabling / disabling dynmaically.....
        WriteConsole(True, "Loading history data from database...")
        Dim FunctionsRecs() As DataRow = GetFunctionsInfo("")
        For Each rec As DataRow In FunctionsRecs
            '''            Exit For       '----------------------------------------------------------------------------------------------------------------
            Dim Duration = ""
            If CStr(rec("Type")) <> "" Then              ' Process functions with a valid history field
                Dim StartDate As New DateTime
                Select Case CStr(rec("Type")).ToUpper()
                    Case Is = "ELAPSED"
                        Select Case CStr(rec("PrevUnit")).ToUpper()
                            Case Is = "MINUTES"
                                StartDate = Date.UtcNow.AddMinutes(-1 * CInt(rec("Prev")))
                            Case Is = "HOURS"
                                StartDate = Date.UtcNow.AddHours(-1 * CInt(rec("Prev")))
                            Case Is = "DAYS"
                                StartDate = Date.UtcNow.AddDays(-1 * CInt(rec("Prev")))
                            Case Is = "WEEKS"
                                StartDate = Date.UtcNow.AddDays(-7 * CInt(rec("Prev")))
                            Case Is = "MONTHS"
                                StartDate = Date.UtcNow.AddMonths(-1 * CInt(rec("Prev")))
                            Case Is = "YEARS"
                                StartDate = Date.UtcNow.AddYears(-1 * CInt(rec("Prev")))
                        End Select
                        Duration = "_" + CStr(rec("PrevUnit")).ToUpper() + CStr(rec("Prev"))        ' Ensure unique record for interval selected
                    Case Is = "SINCE"
                        'TODO
                End Select

                Dim scopeStr = ""
                If CStr(rec("Scope")) <> "" Then scopeStr = " AND SCOPE LIKE '%" + CStr(rec("Scope")) + "%'" ' Allow partial matching
                'Dim CondStr = "CATEGORY=" + CStr(rec("Category")) + " AND CLASS='" + CStr(rec("Class")) + "' AND INSTANCE='" + CStr(rec("Instance")) + "'" + scopeStr + _
                Dim CondStr = "CLASS='" + CStr(rec("Class")) + "' AND INSTANCE='" + CStr(rec("Instance")) + "'" + scopeStr +
                              " AND TIME BETWEEN " + StartDate.Ticks().ToString() + " AND " + Date.UtcNow.Ticks().ToString()
                Dim HistMsg As Structures.HAMessageStruc
                HistMsg.Network = GlobalVars.myNetNum
                HistMsg.Category = HS.GetCatName(CByte(rec("Category")))
                HistMsg.ClassName = CStr(rec("Class"))
                HistMsg.Instance = CStr(rec("Instance"))

                ' TODO: Make the log database columns pascal case not uppercase

                Select Case CStr(rec("History")).ToUpper()                 ' Process function
                    Case Is = "AVERAGE"                                    ' Use weighted averages with time as weight as datapoints are saved on changes not regular datapoints
                        Dim SumVal As Double = 0
                        Dim SumTime As Double = 0
                        Dim WeightAvg As Single = 0

                        ' Get all matching records in the timerange
                        Dim LogRecs As DataRow() = HS.GetLogs("TIME,DATA", CondStr, CInt(rec("Category")) - 1, "TIME ASC")

                        If LogRecs.Count > 0 Then
                            Dim TimeWeight As Long
                            For recCnt As Integer = 0 To LogRecs.Count - 2
                                TimeWeight = CLng((CLng(LogRecs(recCnt + 1)("TIME")) - CLng(LogRecs(recCnt)("TIME"))) / 10000)    ' Delta time between current rec + next rec = time weighting for curr rec, convert from ticks to secs
                                SumVal = SumVal + CDbl(CSng(LogRecs(recCnt)("DATA")) * TimeWeight)          ' weighted average = sum(datapoint * timeslot) / sum(all timeslots)
                                SumTime = CDbl(SumTime + TimeWeight)
                            Next

                            ' As the last record is likely a shutdown use the time delta from the previous record as an approximation for weighting for the last record
                            SumVal = SumVal + CDbl(CSng(LogRecs(LogRecs.Count - 1)("DATA")) * TimeWeight)
                            SumTime = CDbl(SumTime + TimeWeight)
                            WeightAvg = CSng(SumVal / SumTime)

                            ' Store the average weights for future calculations in statestore with _SUMXXX appended
                            HistMsg.Scope = "_SUMVAL" + Duration
                            HistMsg.Data = CStr(SumVal)
                            HS.SetStoreState(HistMsg)                               ' Save the sum of weighted values (* delta time)
                            HistMsg.Scope = "_SUMTIME" + Duration
                            HistMsg.Data = CStr(SumTime)
                            HS.SetStoreState(HistMsg)                               ' Save the sum of times
                            HistMsg.Scope = "_PREVTIME" + Duration
                            HistMsg.Data = CStr(Date.UtcNow.Ticks)                  ' Save the startup time to calculate delta time 
                            HS.SetStoreState(HistMsg)
                            HistMsg.Scope = "_PREVVAL" + Duration
                            HistMsg.Data = CStr(WeightAvg)                  ' For the first time calculating new average, use the weighted value as previous value as we don't know what the previous value was when we first start
                            HS.SetStoreState(HistMsg)
                        End If
                    Case Is = "SUM"

                    Case Is = "COUNT"

                    Case Is = "MAX", "MAXTIMENEWEST", "MAXTIMEOLDEST"
                        ' Get all matching records in the timerange and return maximum value & time recorded
                        HistMsg.Scope = "_MAX" + Duration
                        If IsNothing(HS.GetStoreState(HistMsg)) Then                    ' Don't hit database again if store max already exists
                            'Dim LogRecs As DataRow() = HS.GetLogs("TIME, MAX(CAST(DATA AS REAL)) AS DATA", CondStr, CInt(rec("Category")) - 1)
                            Dim OrderLimit As String = "TIME"
                            Dim SelectStr As String = "TIME, MAX(CAST(DATA AS REAL)) AS DATA"   ' Get oldest (first) maximum value via MAX function
                            If CStr(rec("History")).ToUpper() = "MAXTIMENEWEST" Then SelectStr = "TIME, DATA" : OrderLimit = "DATA DESC, TIME DESC LIMIT 1"     ' Get newest (last) maximum value

                            Dim LogRecs As DataRow() = HS.GetLogs(SelectStr, CondStr, CInt(rec("Category")) - 1, OrderLimit)
                            If LogRecs.Length > 0 AndAlso Not IsDBNull(LogRecs(LogRecs.Count - 1)("DATA")) Then
                                HistMsg.Data = CStr(LogRecs(LogRecs.Count - 1)("DATA"))
                                HS.SetStoreState(HistMsg)                               ' Save the maximum value
                                HistMsg.Scope = "_MAXTIME" + Duration
                                Dim Time As Long                                        ' Convert to javascript time
                                If Long.TryParse(CStr(LogRecs(LogRecs.Count - 1)("TIME")), Globalization.NumberStyles.AllowExponent Or Globalization.NumberStyles.AllowDecimalPoint, Globalization.CultureInfo.CurrentCulture, Time) Then HistMsg.Data = CStr(HAUtils.ToJSTime(Time)) Else HistMsg.Data = ""
                                HS.SetStoreState(HistMsg)                               ' Save the time
                            End If
                        End If

                    Case Is = "MIN", "MINTIMENEWEST", "MINTIMEOLDEST"
                        HistMsg.Scope = "_MIN" + Duration
                        If IsNothing(HS.GetStoreState(HistMsg)) Then                    ' Don't hit database again if store max already exists

                            'Dim OrderLimit As String = "DATA, TIME ASC LIMIT 1"             ' Get oldest (first) max value
                            Dim OrderLimit As String = "TIME"
                            Dim SelectStr As String = "TIME, MIN(CAST(DATA AS REAL)) AS DATA"       ' Get oldest (first) min value via MIN function
                            If CStr(rec("History")).ToUpper() = "MINTIMENEWEST" Then SelectStr = "TIME, DATA" : OrderLimit = "DATA ASC, TIME DESC LIMIT 1"     ' get newest(last) max val

                            'Dim LogRecs As DataRow() = HS.GetLogs("TIME, MIN(CAST(DATA AS REAL)) AS DATA", CondStr, CInt(rec("Category")) - 1)
                            Dim LogRecs As DataRow() = HS.GetLogs(SelectStr, CondStr, CInt(rec("Category")) - 1, OrderLimit)
                            If LogRecs.Count > 0 AndAlso Not IsDBNull(LogRecs(LogRecs.Count - 1)("DATA")) Then
                                HistMsg.Data = CStr(LogRecs(LogRecs.Count - 1)("DATA"))
                                HS.SetStoreState(HistMsg)                               ' Save the minimum value
                                HistMsg.Scope = "_MINTIME" + Duration
                                'HistMsg.Data = CStr(LogRecs(LogRecs.Count - 1)("TIME"))
                                Dim Time As Long
                                If Long.TryParse(CStr(LogRecs(LogRecs.Count - 1)("TIME")), Globalization.NumberStyles.AllowExponent Or Globalization.NumberStyles.AllowDecimalPoint, Globalization.CultureInfo.CurrentCulture, Time) Then HistMsg.Data = CStr(HAUtils.ToJSTime(Time)) Else HistMsg.Data = ""
                                HS.SetStoreState(HistMsg)                               ' Save the minimum time
                            End If
                        End If

                End Select
            End If
        Next

        ' Process all history transforms so state store has current history records when starting
        SyncLock TransformLock
            WriteConsole(True, "Initializing history messages...")
            For Each TransRow As DataRow In FuncsDT.Rows
                'WriteConsole(True, "XXX " + CStr(TransRow("TransFunc")) + " " + CStr(EventMessage.Network) + "   " + CStr(TransRow("Category")) + " " + CStr(TransRow("Class")) + " " + CStr(TransRow("Instance")))
                Dim TestTrans = CStr(TransRow("TransFunc")).ToUpper                              ' Found a match on channel in one of the function records
                If TestTrans <> "" Then          ' Process match
                    Dim TFRow As DataRow() = TransFuncsDT.Select("TFName = '" + TestTrans.Trim() + "'"c)            ' Get the transform record
                    If CBool(TFRow(0)("Enabled")) Then
                        Dim FuncRows As DataRow() = FuncsDT.Select("TransFunc = '" + TestTrans.Trim() + "'", "FuncIndex ASC")       ' Get all functions for the transform
                        Dim CalcVal As Single = 0
                        For Each FuncRow As DataRow In FuncRows                             ' Process each function saved in transform
                            Dim myVal As Single = 0
                            Dim FuncMessage As Structures.HAMessageStruc
                            If IsDBNull(FuncRow("Val")) Then                                ' If Val field is null, process a function message
                                FuncMessage.Network = CByte(FuncRow("Network"))
                                FuncMessage.Category = HS.GetCatName(CByte(FuncRow("Category")))
                                FuncMessage.ClassName = CStr(FuncRow("Class"))
                                FuncMessage.Instance = CStr(FuncRow("Instance"))
                                FuncMessage.Scope = CStr(FuncRow("Scope"))
                                Dim Duration = "_" + CStr(FuncRow("PrevUnit")).ToUpper() + CStr(FuncRow("Prev"))

                                Dim HistFunc = CStr(FuncRow("History")).ToUpper()
                                Select Case HistFunc
                                    Case Is = "AVERAGE"
                                        FuncMessage.Scope = "_SUMVAL" + Duration
                                        Dim SumVal As Double = CDbl(HS.GetStoreState(FuncMessage))
                                        FuncMessage.Scope = "_SUMTIME" + Duration
                                        Dim SumTime As Double = CDbl(HS.GetStoreState(FuncMessage))
                                        If SumTime <> 0 Then myVal = CSng(SumVal / SumTime) Else myVal = 0 ' Weighted average

                                    Case Is = "SUM"

                                    Case Is = "COUNT"

                                    Case Is = "MAX", "MAXTIMENEWEST", "MAXTIMEOLDEST"
                                        FuncMessage.Scope = ""                          ' Check all values in the statestore for the channel
                                        Dim foundKeys As Dictionary(Of Structures.StateStoreKey, String) = HS.SearchStoreStates(FuncMessage)
                                        Dim testSng As Single
                                        For Each Key In foundKeys
                                            If Single.TryParse(Key.Value, Globalization.NumberStyles.AllowExponent Or Globalization.NumberStyles.AllowDecimalPoint, Globalization.CultureInfo.CurrentCulture, testSng) Then
                                                If HistFunc = "MAX" And Key.Key.Scope = "_MAX" + Duration Then
                                                    myVal = testSng ' historical max value (by duration)
                                                Else
                                                    If Key.Key.Scope = "_MAXTIME" + Duration Then myVal = testSng
                                                End If
                                            End If
                                        Next

                                    Case Is = "MIN", "MINTIMENEWEST", "MINTIMEOLDEST"
                                        FuncMessage.Scope = ""                          ' Check all values in the statestore for the channel
                                        Dim foundKeys As Dictionary(Of Structures.StateStoreKey, String) = HS.SearchStoreStates(FuncMessage)
                                        Dim testSng As Single
                                        For Each Key In foundKeys
                                            If Single.TryParse(Key.Value, Globalization.NumberStyles.AllowExponent Or Globalization.NumberStyles.AllowDecimalPoint, Globalization.CultureInfo.CurrentCulture, testSng) Then
                                                If HistFunc = "MIN" And Key.Key.Scope = "_MIN" + Duration Then
                                                    myVal = testSng ' historical max value (by duration)
                                                Else
                                                    If Key.Key.Scope = "_MINTIME" + Duration Then myVal = testSng
                                                End If
                                            End If
                                        Next
                                        Dim tt = 1
                                End Select
                            Else
                                Single.TryParse(CStr(FuncRow("Val")), myVal)                ' Val is a number
                            End If
                            CalcVal = ApplyOper(CalcVal, CStr(FuncRow("FuncType")), myVal)  ' calculate value based on function
                            WriteConsole(False, TestTrans + " function: " + CStr(FuncRow("FuncType")) + " Incremental value: " + CStr(CalcVal))
                        Next

                        ' Apply rounding to the result, and create new event message
                        'WriteConsole(False, "Transform created message: " + HS.GetCatName(CByte(TFRow(0)("Category"))) + "/" + CStr(TFRow(0)("Class")) + " " + CStr(TFRow(0)("Instance")) + ":" + CStr(CalcVal) + " (" + CStr(TFRow(0)("Scope")) + ")")
                        HS.CreateMessage(CStr(TFRow(0)("Class")), HAConst.MessFunc.EVENT, HAConst.MessLog.NORMAL, CStr(TFRow(0)("Instance")), CStr(TFRow(0)("Scope")), _
                                         CStr(Math.Round(CalcVal, CInt(TFRow(0)("RoundDec")), MidpointRounding.AwayFromZero)), HS.GetCatName(CByte(TFRow(0)("Category"))), GlobalVars.myNetNum)
                    End If
                End If
            Next

        End SyncLock

    End Function

    ' Process automation events, test each trigger for a match to the event message, then if there is an event attached to the trigger run the actions associated with that event
    Public Shared Function AutomationEvents(EventMessage As Structures.HAMessageStruc) As Boolean
        Dim EventRun As New ArrayList                                                                                               ' Keep a list of the events that have been run so we don't end up ServiceState.RUNNING the event twice if different triggers fire
        SyncLock AccessDB
            For Each TrigRow As DataRow In TriggersDT.Rows                                                                              ' Loop through each trigger looking for a match (use trigger list not eventtriggers as there may be multiple events using the same trigger)

                If MatchTrigger(TrigRow, EventMessage) Then                                                                             ' Got a match, so check to see what events have this trigger registered and run all their actions

                    For Each EventTriggerRow In EventTriggersDT.Select("TrigName = '" + CStr(TrigRow("TrigName")) + "'"c)                ' Get all the events that are registered with this trigger
                        Dim myEvent() As DataRow = GetEventsInfo(CStr(EventTriggerRow("EventName")))                                     ' Get the event information

                        If myEvent.Count <> 0 And myEvent(0) IsNot Nothing Then                                                                                      ' Check that the event actually exists
                            If CBool(myEvent(0).Item("EventActive")) Then                                                               ' Is the event enabled
                                Dim EventName As String = CStr(myEvent(0).Item("EventName"))

                                ' Run each action associated with the event
                                If EventRun.IndexOf(EventName) = -1 Then                                                                ' Has this event already been run in this instance? (returns -1 if eventname is not in the list, so has not been run)
                                    EventRun.Add(EventName)                                                                             ' Save this event name to the run list so that the event isn't run several times

                                    ' Check to see if we are within the active times for the event, if not, skip to the next trigger
                                    If Date.FromBinary(CLng(myEvent(0).Item("EventStart"))) > Date.Now Then Continue For
                                    If CLng(myEvent(0).Item("EventStop")) <> 0 Then If Date.FromBinary(CLng(myEvent(0).Item("EventStop"))) < Date.Now Then Continue For       ' null date is 0 so check it first

                                    Dim RunActions As String = "Actions: "
                                    Dim myEventActions() As DataRow = GetEventActionsInfo(EventName)                                    ' Get the actions associated with the event
                                    For Each EventAction As DataRow In myEventActions                                                   ' Loop through all the actions tagged to the event
                                        Dim myActions() As DataRow = GetActionsInfo(CStr(EventAction.Item("ActionName")))
                                        RunActions = RunActions + CStr(myActions(0).Item("ActionName")) + ", "                          ' Record the actions processed
                                        System.Threading.ThreadPool.QueueUserWorkItem(AddressOf ProcessAction, myActions(0))            ' Multithreaded with threadpool (25 threads per CPU concurrent max)
                                    Next
                                    HS.CreateMessage("EVENT", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "RUN", EventName, RunActions.Substring(0, RunActions.Length - 2), "SYSTEM")

                                    ' When an event is active as the triggers have fired, update the event last fired & number of times fields in the DB
                                    Dim RowLocn As Integer = EventsDT.Rows.IndexOf(myEvent(0))
                                    'SyncLock AccessDB
                                    If CBool(myEvent(0).Item("EventOneOff")) Then EventsDT(RowLocn).Item("EventActive") = False ' If the event is a single shot (one off), then disable the event so it is only run once
                                    EventsDT(RowLocn).Item("EventLastFired") = Date.UtcNow.Ticks
                                    EventsDT(RowLocn).Item("EventNumRecur") = CInt(EventsDT(RowLocn).Item("EventNumRecur")) + 1
                                    'End SyncLock
                                    UpdateAutoDB("UPD", "EVENTS", EventsDT(RowLocn))

                                End If
                            End If
                        Else
                            WriteConsole(False, "WARNING - Inconsistency in automation DB, cannot find event action for trigger: " + CStr(TrigRow("TrigName")))
                        End If
                    Next
                End If
            Next
        End SyncLock

        Return True
    End Function

    ' THREAD: This routine processes the specific action and called when an event condition is met (called from trigger processing and manually from the UI)
    Public Shared Sub ProcessAction(stateInfo As Object)
        Try
            Dim ActionRow As DataRow = DirectCast(stateInfo, DataRow)                                                               ' Thread handler cannot pass strings, just objects, so convert to DataRow object

            ' Make the thread wait a random number of seconds up to the number specified in ActionDelay
            Dim ActionDelay As Integer = CInt(ActionRow("ActionDelay"))
            If CBool(ActionRow("ActionRandom")) = True And ActionDelay <> 0 Then
                Dim rnd As New Random(System.DateTime.Now.Millisecond)                                                              ' Generate random number seed
                ActionDelay = rnd.Next(1, ActionDelay)                                                                              ' Get a random number between 1 sec and ActionDelay secs
            End If
            If ActionDelay <> 0 Then System.Threading.Thread.Sleep(ActionDelay * 1000) ' Delay is in seconds

            ' Process Scripts
            If CStr(ActionRow("ActionScript")) <> "" Then
                WriteConsole(False, "Running script: " + CStr(ActionRow("ActionScript")) + "(" + CStr(ActionRow("ActionScriptParam")) + ")")
                Dim ScriptResult As String = HS.RunScript(CStr(ActionRow("ActionScript")), CStr(ActionRow("ActionScriptParam"))) ' Run the script specified in the trigger
            End If

            ' Send a Message if it is specified
            If CStr(ActionRow("ActionData")) <> "" Then
                HS.CreateMessage(CStr(ActionRow("ActionClass")), HAConst.MessFunc.EVENT, CByte(ActionRow("ActionLogLevel")), CStr(ActionRow("ActionInstance")), CStr(ActionRow("ActionScope")), CStr(ActionRow("ActionData")), HS.GetCatName(CByte(ActionRow("ActionCategory"))), CByte(ActionRow("ActionNetwork")))
            End If
        Catch ex As Exception                                                                                                       ' THis is the top level exception as we are ServiceState.RUNNING on a separate thread
            HS.HandleAppErrors(HAConst.MessLog.MAJOR, "SYSTEM", "AUTOMATION", "ProcessAction", "ERROR", "Error in processing actions. ", ex)
        End Try

    End Sub

    ' Match a trigger for a message event. Use test for a negative condition which will exit the function, saving CPU resources. Put most likely exits first
    Private Shared Function MatchTrigger(Trigger As DataRow, EventMessage As Structures.HAMessageStruc) As Boolean

        MatchTrigger = False
        ' Check interrupts first. Check to see if the interrupt corresponds to the interrupt flag
        If MatchMessage(Trigger, EventMessage) = False Then Exit Function

        ' [AND] Check data state records (only if they exist in the trigger record)
        If MatchState(Trigger) = False Then Exit Function

        ' [AND] Check the time trigger info for a match (NOTE: will return true if no time flags are set, ie. we pass the time checks)
        If MatchTime(Trigger) = False Then Exit Function

        ' [AND] Run a script associated with the trigger and test its return value
        If MatchScript(Trigger) = False Then Exit Function

        ' Update Trigger Last fired field (different to event last fired)
        Dim RowLocn As Integer = TriggersDT.Rows.IndexOf(Trigger)
        If RowLocn = -1 Then Return False ' Can't locate the row to update
        'SyncLock AccessDB
        TriggersDT(RowLocn).Item("TrigLastFired") = Date.UtcNow.Ticks
        'End SyncLock
        'TODO: Put this on a separate thread
        UpdateAutoDB("UPD", "TRIGGERS", TriggersDT(RowLocn), RowLocn)

        Return True
    End Function

    ' Run a script associated with the trigger, returning the result of the script
    Private Shared Function MatchScript(row As DataRow) As Boolean
        If CStr(row("TrigScript")) <> "" Then                      ' Don't process if there is no script entry
            Dim ScriptResult As String = ""
            ScriptResult = HS.RunScript(CStr(row("TrigScript")), CStr(row("TrigScriptParam")))  ' Run the script specified in the trigger with a parameter
            Return TestData(CInt(row("TrigScriptCond")), CStr(row("TrigScriptData")), ScriptResult)     ' Return true or false depending if the script results = the data field in the trigger for the script
        Else
            Return True                                                                                 ' No script, so return true
        End If
    End Function

    ' Check to see if the time triggers are matched (called for each event as well as each minute change). To keep logic simple & fast, test for the negative so that we don't need to test every condition
    Private Shared Function MatchTime(row As DataRow) As Boolean
        MatchTime = False

        Dim aa = Date.UtcNow
        Dim xx = Date.FromBinary(CLng(row.Item("TrigDateFrom")))
        Dim yy = Date.FromBinary(CLng(row.Item("TrigTimeFrom"))).TimeOfDay.TotalMinutes
        Dim zz = Date.FromBinary(CLng(row.Item("TrigDateFrom"))).AddMinutes(Date.FromBinary(CLng(row.Item("TrigTimeFrom"))).TimeOfDay.TotalMinutes)
        Dim xxx = Date.FromBinary(CLng(row.Item("TrigDateTo")))
        Dim yyy = Date.FromBinary(CLng(row.Item("TrigTimeTo"))).TimeOfDay.TotalMinutes
        Dim zzz = Date.FromBinary(CLng(row.Item("TrigDateTo"))).AddMinutes(Date.FromBinary(CLng(row.Item("TrigTimeTo"))).TimeOfDay.TotalMinutes)

        ' TODO: Not reading from / to dates correctly as when converting to UTC we lose a day due to time difference (as time isn't added). So when just dealing with dates, don't use UTC. Need to adjust the below
        If CLng(row.Item("TrigDateFrom")) <> 0 Then
            If Date.FromBinary(CLng(row.Item("TrigDateFrom"))).Date.AddMinutes(Date.FromBinary(CLng(row.Item("TrigTimeFrom"))).TimeOfDay.TotalMinutes) > Date.UtcNow Then Exit Function ' Current time is before the 'from' Date/Time, so exit
        Else    ' Start date not set, so check if the stop time is set and it is after the stop time, exit sub
            If CLng(row.Item("TrigTimeFrom")) <> 0 Then If Date.FromBinary(CLng(row.Item("TrigTimeFrom"))).TimeOfDay.TotalMinutes > Date.UtcNow.TimeOfDay.TotalMinutes Then Exit Function
        End If
        If CLng(row.Item("TrigDateTo")) <> 0 Then
            If Date.FromBinary(CLng(row.Item("TrigDateTo"))).Date.AddMinutes(Date.FromBinary(CLng(row.Item("TrigTimeTo"))).TimeOfDay.TotalMinutes) < Date.UtcNow Then Exit Function ' Current time is after the 'to' Date/Time
        Else    ' Stop date not set, so check if the stop time is set and it is after the stop time, exit sub
            If CLng(row.Item("TrigTimeTo")) <> 0 Then If Date.FromBinary(CLng(row.Item("TrigTimeTo"))).TimeOfDay.TotalMinutes < Date.UtcNow.TimeOfDay.TotalMinutes Then Exit Function
        End If

        ' If a weekday is set and today is not the day, exit
        If CInt(row("TrigMon")) + CInt(row("TrigTue")) + CInt(row("TrigWed")) + CInt(row("TrigThu")) + CInt(row("TrigFri")) + CInt(row("TrigSat")) + CInt(row("TrigSun")) > 0 Then
            Select Case Now().DayOfWeek.ToString.ToUpper
                Case Is = "MONDAY"
                    If CBool(row("TrigMon")) = False Then Exit Function
                Case Is = "TUESDAY"
                    If CBool(row("TrigTue")) = False Then Exit Function
                Case Is = "WEDNESDAY"
                    If CBool(row("TrigWed")) = False Then Exit Function
                Case Is = "THURSDAY"
                    If CBool(row("TrigThu")) = False Then Exit Function
                Case Is = "FRIDAY"
                    If CBool(row("TrigFri")) = False Then Exit Function
                Case Is = "SATURDAY"
                    If CBool(row("TrigSat")) = False Then Exit Function
                Case Is = "SUNDAY"
                    If CBool(row("TrigSun")) = False Then Exit Function
            End Select

            ' If we have selected a day, then we can set the frequency (eg. 1st Tuesday of every month), checking the last time the event fired (will fire the first time as null date is a long time ago)
            If CInt(row("TrigFortnightly")) + CInt(row("TrigMonthly")) + CInt(row("TrigYearly")) > 0 Then
                If CBool(row("TrigFortnightly")) = True And DateTime.Compare(Now().AddDays(-14), Date.FromBinary(CLng(row.Item("TrigLastFired")))) < 0 Then Exit Function
                If CBool(row("TrigMonthly")) = True And DateTime.Compare(Now().AddMonths(-1), Date.FromBinary(CLng(row.Item("TrigLastFired")))) < 0 Then Exit Function
                If CBool(row("TrigYearly")) = True And DateTime.Compare(Now().AddYears(-1), Date.FromBinary(CLng(row.Item("TrigLastFired")))) < 0 Then Exit Function
            End If
        End If

        If HS.IsNight = False And CBool(row("TrigNightTime")) = True Then Exit Function ' Does this only fire at Night? 
        If HS.IsDay = False And CBool(row("TrigDaytime")) = True Then Exit Function ' Does this only fire during the Day?

        ' If sunrise/set is set and it is not sunrise/set, exit. Note IsSunrise/set is a duration (sunrise/set +- offset), if you want to do something based on a sunrise/set event, use the sunrise/set message event not IsSunrise/set
        If HS.IsSunrise = False And CBool(row("TrigSunrise")) = True Then Exit Function
        If HS.IsSunset = False And CBool(row("TrigSunset")) = True Then Exit Function

        ' Check the time of day (only trigger once in the minute by checking last trig time)
        Dim TrigTimeOfDay = CLng(row.Item("TrigTimeOfDay"))
        If TrigTimeOfDay <> 0 Then
            Dim TrigDate = Date.FromBinary(TrigTimeOfDay).ToLocalTime()
            If Date.FromBinary(TrigTimeOfDay).ToLocalTime.Hour <> Date.Now.Hour Then Exit Function
            If Date.FromBinary(TrigTimeOfDay).ToLocalTime.Minute <> Date.Now.Minute Then Exit Function
            If Date.FromBinary(CLng(row.Item("TrigLastFired"))).ToLocalTime.Day = Date.Now.Day Then Exit Function        ' Only trigger once per day
        End If

        MatchTime = True                                        ' Made it here, so the time trigger is valid
    End Function

    ' Test the message state cache if the current state is met
    Private Shared Function MatchState(row As DataRow) As Boolean
        Dim MatchMessage As New Structures.HAMessageStruc
        With MatchMessage
            .Network = CByte(row("TrigStateNetwork"))
            .Category = HS.GetCatName(CByte(row("TrigStateCategory")))
            .ClassName = CStr(row("TrigStateClass"))
            .Instance = CStr(row("TrigStateInstance"))
            .Scope = CStr(row("TrigStateScope"))
            .Data = CStr(row("TrigStateData"))
        End With
        If MatchMessage.Network = 0 And MatchMessage.Category = "ALL" And MatchMessage.ClassName = "" And MatchMessage.Instance = "" And MatchMessage.Scope = "" Then Return True
        ' XXXXXXX NEED TO TEST FOR DATA LESS THAN, GREATER THAN etc.
        Dim ResultsDict As Dictionary(Of Structures.StateStoreKey, String) = HAConsole.HS.SearchStoreStates(MatchMessage)           ' Search the state store for partial key matches to the trigger state values
        If IsNothing(ResultsDict) Then Return False ' If nothing is returned then the key isn't found
        If ResultsDict.ContainsValue(MatchMessage.Data) Then Return True Else Return False ' The key is found, so check to see if the value is in the found keys, if it is return success
    End Function

    ' Test an event message against a trigger to see if it is a match. For efficient processing, the routine exits as soon as any of the fields don't match (Treat empty strings as wildcards)
    Private Shared Function MatchMessage(row As DataRow, EventMessage As Structures.HAMessageStruc) As Boolean
        MatchMessage = False
        If CStr(row("TrigChgClass")) <> "" Then If CStr(row("TrigChgClass")).ToUpper <> EventMessage.ClassName.ToUpper Then Exit Function ' Don't test if the field is empty (treat as wildcard)
        If CStr(row("TrigChgInstance")) <> "" Then If CStr(row("TrigChgInstance")).ToUpper <> EventMessage.Instance.ToUpper Then Exit Function
        'If CStr(row("TrigChgScope")) <> "" Then If CStr(row("TrigChgScope")).ToUpper <> EventMessage.Scope.ToUpper Then Exit Function
        If CStr(row("TrigChgScope")) <> "" Then If EventMessage.Scope.IndexOf(CStr(row("TrigChgScope")), StringComparison.OrdinalIgnoreCase) = -1 Then Exit Function ' allow partial match for scope
        If CStr(row("TrigChgData")) = "" Then MatchMessage = True : Exit Function ' If no data is specified and we get this far, then we have a match
        If TestData(CInt(row("TrigChgCond")), CStr(row("TrigChgData")), EventMessage.Data) = False Then Exit Function
        If CByte(row("TrigChgCategory")) <> 0 Then If HS.GetCatName(CByte(row("TrigChgCategory"))) <> EventMessage.Category Then Exit Function ' Don't test for the category field if cateogry = 0 (All Categories)
        If CByte(row("TrigChgNetwork")) <> 0 Then If CByte(row("TrigChgNetwork")) <> EventMessage.Network Then Exit Function ' Don't test for the network field if network = 0 (All networks)
        MatchMessage = True                                                                                             ' Made it this far, so the event trigger is a match
    End Function

    ' Helper function that takes 2 strings and tests them against the condition, adjusting for either string or numerical
    Private Shared Function TestData(TestCond As Integer, TrigData As String, TestWith As String) As Boolean
        TestData = False
        Dim TrigVal As Single = 0, EventVal As Single = 0
        Select Case TestCond                                                                                ' Check data, either string matches or number
            Case Is = HAConst.TestCond.EQUALS
                If TrigData.ToUpper <> TestWith.ToUpper Then Exit Function ' String or numeric values, ignore case
            Case Is = HAConst.TestCond.NOT_EQUAL
                If TrigData.ToUpper = TestWith.ToUpper Then Exit Function ' String or numeric values
            Case Is = HAConst.TestCond.GREATER_THAN                                                          ' Assume numeric, and strip out any extra data after a ',' delimiter
                If Single.TryParse(TrigData, TrigVal) And Single.TryParse(TestWith, EventVal) Then          ' Check that I have a valid number in both DB & trigger fields
                    If TrigVal >= EventVal Then Exit Function ' Exit if the event value is less than the trigger value 
                Else
                    Exit Function                                                                           ' Either number is invalid, so don't match
                End If
            Case Is = HAConst.TestCond.LESS_THAN
                If Single.TryParse(TrigData, TrigVal) And Single.TryParse(TestWith, EventVal) Then          ' Check that I have a valid number in both DB & trigger fields
                    If TrigVal <= EventVal Then Exit Function ' Exit if the event value is greater than the trigger value 
                Else
                    Exit Function                                                                           ' Either number is invalid, so don't match
                End If
        End Select
        TestData = True                                                                                     ' Made it this far so we have a match
    End Function

#End Region
    '#####################################################################################################################################################################################
#Region "DB Requests"

    Public Shared Function ProcessGetMsg(Table As String, Data As String) As List(Of Object)
        Dim GetRow As DataRow() = Nothing
        Dim GetColl As New List(Of Object)
        Select Case Table.ToUpper
            Case Is = "TRIGGERS"
                GetRow = GetTriggersInfo(Data)
            Case Is = "ACTIONS"
                GetRow = GetActionsInfo(Data)
            Case Is = "EVENTS"
                GetRow = GetEventsInfo(Data)
            Case Is = "EVENTACTIONS"
                Dim GetTable As DataTable = GetEventActionsInfo(Data).CopyToDataTable
                GetTable.Columns.Add("Desc")                            ' Add desc column via datatable
                GetRow = GetTable.Select()
                Dim i As Int16 = 0
                For Each row As DataRow In GetRow
                    Dim rowAction As DataRow() = GetActionsInfo(row.Item("ActionName").ToString)       ' Extract the trigger description column and add it to the datarow
                    GetRow(i).Item("Desc") = rowAction(0).Item("ActionDescription")
                    i = CShort(i + 1)
                Next
            Case Is = "EVENTTRIGGERS"
                Dim GetTable As DataTable = GetEventTriggersInfo(Data).CopyToDataTable
                GetTable.Columns.Add("Desc")                            ' Add desc column via datatable
                GetRow = GetTable.Select()
                Dim i As Int16 = 0
                For Each row As DataRow In GetRow
                    Dim rowTrig As DataRow() = GetTriggersInfo(row.Item("TrigName").ToString)       ' Extract the trigger description column and add it to the datarow
                    GetRow(i).Item("Desc") = rowTrig(0).Item("TrigDescription")
                    i = CShort(i + 1)
                Next
            Case Is = "TRANSFORMS"
                GetRow = GetTransformsInfo(Data)
            Case Is = "FUNCS"
                GetRow = GetFunctionsInfo(Data)
            Case Is = "NAMES"
                GetRow = GetNames(Data)               ' Tablename is data
            Case Is = "WIDGETS"
                Dim FindFiles() As String = Directory.GetFiles(HS.ClientLocn + "\widgets", Data + "*")
                If FindFiles.Length > 0 Then
                    Dim GetTable As New DataTable
                    Dim row As DataRow = GetTable.NewRow()
                    GetTable.Columns.Add("Name")
                    GetTable.Columns.Add("Desc")
                    GetTable.Columns.Add("Lang")
                    GetTable.Columns.Add("Code")
                    row.Item("Name") = Data
                    If FindFiles(0).IndexOf("_") <> -1 Then
                        Dim TempStr As String = FindFiles(0).Substring(FindFiles(0).IndexOf("_") + 1)
                        row("Desc") = TempStr.Substring(0, TempStr.LastIndexOf("."))
                    End If
                    row.Item("Lang") = FindFiles(0).Substring(FindFiles(0).LastIndexOf(".") + 1).ToUpper
                    row.Item("Code") = File.ReadAllText(FindFiles(0))
                    GetTable.Rows.Add(row)
                    GetTable.AcceptChanges()
                    GetRow = GetTable.Select()
                End If
            Case Else
                Return GetColl
        End Select
        If IsNothing(GetRow) Then Return Nothing
        For Each dr As DataRow In GetRow
            Dim DictRow As IDictionary(Of String, Object) = dr.Table.Columns.Cast(Of DataColumn)().ToDictionary(Function(col) col.ColumnName, Function(col) dr.Field(Of Object)(col.ColumnName))
            GetColl.Add(DictRow)
        Next
        Return GetColl
    End Function

    ' All dates coming to the server must be in .NET binary date format UTC, so force UTC kind
    Public Shared Function UTCKind(UTC As String) As DateTime
        'Dim yy = Date.FromBinary(CLng(UTC))
        'Dim xx = DateTime.SpecifyKind(Date.FromBinary(CLng(UTC)), DateTimeKind.Utc)
        Return DateTime.SpecifyKind(Date.FromBinary(CLng(UTC)), DateTimeKind.Utc)
    End Function

    Public Shared Function ProcessAddMsg(Table As String, Data As System.Collections.Generic.List(Of Object)) As String

        ' TODO: Categories are coming across as numbers but typed as strings
        Dim DataArray As IDictionary(Of String, Object) = CType(Data(0), IDictionary(Of String, Object))
        Dim Result As String = ""
        Select Case Table.ToUpper
            Case Is = "TRIGGERS"
                Dim TrigChgMessage, TrigStateMessage As Structures.HAMessageStruc
                TrigStateMessage.Network = CByte(DataArray.Item("trigStateNetwork"))
                TrigStateMessage.Category = CStr(DataArray.Item("trigStateCategory"))
                TrigStateMessage.ClassName = CStr(DataArray.Item("trigStateClass"))
                TrigStateMessage.Instance = CStr(DataArray.Item("trigStateInstance"))
                TrigStateMessage.Scope = CStr(DataArray.Item("trigStateScope"))
                TrigStateMessage.Data = CStr(DataArray.Item("trigStateData"))
                TrigChgMessage.Network = CByte(DataArray.Item("trigChgNetwork"))
                TrigChgMessage.Category = CStr(DataArray.Item("trigChgCategory"))
                TrigChgMessage.ClassName = CStr(DataArray.Item("trigChgClass"))
                TrigChgMessage.Instance = CStr(DataArray.Item("trigChgInstance"))
                TrigChgMessage.Scope = CStr(DataArray.Item("trigChgScope"))
                TrigChgMessage.Data = CStr(DataArray.Item("trigChgData"))
                Result = AddNewTrigger(CStr(DataArray.Item("trigName")), CStr(DataArray.Item("trigDesc")), CStr(DataArray.Item("trigScriptName")), CStr(DataArray.Item("trigScriptParam")), CInt(DataArray.Item("trigScriptCond")), CStr(DataArray.Item("trigScriptValue")), CInt(CStr(DataArray.Item("trigChgCond"))),
                                  CInt(CStr(DataArray.Item("trigStateCond"))), UTCKind(CStr(DataArray.Item("trigFromDate"))), UTCKind(CStr(DataArray.Item("trigFromTime"))), UTCKind(CStr(DataArray.Item("trigToDate"))), UTCKind(CStr(DataArray.Item("trigToTime"))), CBool(CStr(DataArray.Item("trigChkSunrise"))), CBool(DataArray.Item("trigChkSunset")), CBool(DataArray.Item("trigChkMon")), CBool(DataArray.Item("trigChkTue")), CBool(DataArray.Item("trigChkWed")),
                                  CBool(DataArray.Item("trigChkThu")), CBool(DataArray.Item("trigChkFri")), CBool(DataArray.Item("trigChkSat")), CBool(DataArray.Item("trigChkSun")), CBool(DataArray.Item("trigChkDay")), CBool(DataArray.Item("trigChkNight")), CBool(DataArray.Item("trigChkFortnight")),
                                  CBool(DataArray.Item("trigChkMonth")), CBool(DataArray.Item("trigChkYear")), CBool(DataArray.Item("trigChkActive")), CBool(DataArray.Item("trigChkInactive")), UTCKind(CStr(DataArray.Item("trigTimeofDay"))), TrigChgMessage, TrigStateMessage)
            Case Is = "ACTIONS"
                Dim ActionMessage As Structures.HAMessageStruc
                ActionMessage.Network = CByte(DataArray.Item("actionNetwork"))
                ActionMessage.Category = CStr(DataArray.Item("actionCategory"))
                ActionMessage.ClassName = CStr(DataArray.Item("actionClass"))
                ActionMessage.Instance = CStr(DataArray.Item("actionInstance"))
                ActionMessage.Scope = CStr(DataArray.Item("actionScope"))
                ActionMessage.Data = CStr(DataArray.Item("actionData"))
                Dim delay As Integer = 0
                Integer.TryParse(CStr(DataArray.Item("actionDelay")), delay)
                Result = AddNewAction(CStr(DataArray.Item("actionName")), CStr(DataArray.Item("actionDesc")), delay, CBool(DataArray.Item("actionChkRnd")), CStr(DataArray.Item("actionScriptName")), CStr(DataArray.Item("actionScriptParam")), ActionMessage)
            Case Is = "EVENTS"
                Dim ActionNames As String() = CType(DataArray.Item("eventActions"), List(Of Object)).Cast(Of String).ToArray()
                Dim TrigNames As String() = CType(DataArray.Item("eventTrigs"), List(Of Object)).Cast(Of String).ToArray()
                Result = AddNewEvent(CStr(DataArray.Item("eventName")), CStr(DataArray.Item("eventDesc")), CBool(DataArray.Item("eventChkEnabled")), CBool(DataArray.Item("eventChkOneOff")), Date.FromBinary(CLng(DataArray.Item("eventFromDateTime"))), Date.FromBinary(CLng(DataArray.Item("eventToDateTime"))), TrigNames, ActionNames)
            Case Is = "TRANSFORMS"
                Dim Functions As String() = CType(DataArray.Item("transFuncFunctions"), List(Of Object)).Cast(Of String).ToArray()
                Dim TransformMessage As Structures.HAMessageStruc
                TransformMessage.Network = CByte(DataArray.Item("transFuncOutNetwork"))
                TransformMessage.Category = CStr(DataArray.Item("transFuncOutCategory"))
                TransformMessage.ClassName = CStr(DataArray.Item("transFuncOutClass"))
                TransformMessage.Instance = CStr(DataArray.Item("transFuncOutInstance"))
                TransformMessage.Scope = CStr(DataArray.Item("transFuncOutScope"))
                Result = AddNewTransform(CStr(DataArray.Item("TransformName")), CStr(DataArray.Item("TransformDesc")), CBool(DataArray.Item("transFuncOutChk")), CInt(DataArray.Item("transFuncOutRounding")), TransformMessage, Functions)
                If Result = "OK" Then                                   ' Add new channel
                    Dim newCh As New Structures.ChannelStruc
                    newCh.Name = TransformMessage.Instance
                    newCh.Desc = CStr(DataArray.Item("TransformDesc"))
                    newCh.IO = "output"
                    newCh.Units = TransformMessage.Scope
                    newCh.Type = "TRANSFORM"
                    newCh.Value = ""
                    newCh.Attribs = New List(Of Structures.ChannelAttribStruc)
                    HS.modChannel("ADD", HS.GetCatName(CByte(TransformMessage.Category)), TransformMessage.ClassName, newCh)
                End If
        End Select
        Return Result
    End Function

    Public Shared Function ProcessUpdMsg(Table As String, Data As System.Collections.Generic.List(Of Object)) As String
        Dim Result As String = ""
        Dim DataArray As IDictionary(Of String, Object) = CType(Data(0), IDictionary(Of String, Object))
        Select Case Table.ToUpper
            Case Is = "TRIGGERS"
                Dim TrigChgMessage, TrigStateMessage As Structures.HAMessageStruc
                TrigStateMessage.Network = CByte(DataArray.Item("trigStateNetwork"))
                TrigStateMessage.Category = CStr(DataArray.Item("trigStateCategory"))
                TrigStateMessage.ClassName = CStr(DataArray.Item("trigStateClass"))
                TrigStateMessage.Instance = CStr(DataArray.Item("trigStateInstance"))
                TrigStateMessage.Scope = CStr(DataArray.Item("trigStateScope"))
                TrigStateMessage.Data = CStr(DataArray.Item("trigStateData"))
                TrigChgMessage.Network = CByte(DataArray.Item("trigChgNetwork"))
                TrigChgMessage.Category = CStr(DataArray.Item("trigChgCategory"))
                TrigChgMessage.ClassName = CStr(DataArray.Item("trigChgClass"))
                TrigChgMessage.Instance = CStr(DataArray.Item("trigChgInstance"))
                TrigChgMessage.Scope = CStr(DataArray.Item("trigChgScope"))
                TrigChgMessage.Data = CStr(DataArray.Item("trigChgData"))
                Result = UpdateTrigger(CStr(DataArray.Item("trigName")), CStr(DataArray.Item("trigDesc")), CStr(DataArray.Item("trigScriptName")), CStr(DataArray.Item("trigScriptParam")), CInt(DataArray.Item("trigScriptCond")), CStr(DataArray.Item("trigScriptValue")), CInt(CStr(DataArray.Item("trigChgCond"))),
                                  CInt(CStr(DataArray.Item("trigStateCond"))), UTCKind(CStr(DataArray.Item("trigFromDate"))), UTCKind(CStr(DataArray.Item("trigFromTime"))), UTCKind(CStr(DataArray.Item("trigToDate"))), UTCKind(CStr(DataArray.Item("trigToTime"))), CBool(CStr(DataArray.Item("trigChkSunrise"))), CBool(DataArray.Item("trigChkSunset")), CBool(DataArray.Item("trigChkMon")), CBool(DataArray.Item("trigChkTue")), CBool(DataArray.Item("trigChkWed")),
                                  CBool(DataArray.Item("trigChkThu")), CBool(DataArray.Item("trigChkFri")), CBool(DataArray.Item("trigChkSat")), CBool(DataArray.Item("trigChkSun")), CBool(DataArray.Item("trigChkDay")), CBool(DataArray.Item("trigChkNight")), CBool(DataArray.Item("trigChkFortnight")),
                                  CBool(DataArray.Item("trigChkMonth")), CBool(DataArray.Item("trigChkYear")), CBool(DataArray.Item("trigChkActive")), CBool(DataArray.Item("trigChkInactive")), UTCKind(CStr(DataArray.Item("trigTimeofDay"))), TrigChgMessage, TrigStateMessage)
            Case Is = "ACTIONS"
                Dim ActionMessage As Structures.HAMessageStruc
                ActionMessage.Network = CByte(DataArray.Item("actionNetwork"))
                ActionMessage.Category = CStr(DataArray.Item("actionCategory"))
                ActionMessage.ClassName = CStr(DataArray.Item("actionClass"))
                ActionMessage.Instance = CStr(DataArray.Item("actionInstance"))
                ActionMessage.Scope = CStr(DataArray.Item("actionScope"))
                ActionMessage.Data = CStr(DataArray.Item("actionData"))
                Dim delay As Integer = 0
                Integer.TryParse(CStr(DataArray.Item("actionDelay")), delay)
                Result = UpdateAction(CStr(DataArray.Item("actionName")), CStr(DataArray.Item("actionDesc")), delay, CBool(DataArray.Item("actionChkRnd")), CStr(DataArray.Item("actionScriptName")), CStr(DataArray.Item("actionScriptParam")), ActionMessage)
            Case Is = "EVENTS"
                Dim ActionNames As String() = CType(DataArray.Item("eventActions"), List(Of Object)).Cast(Of String).ToArray()
                Dim TrigNames As String() = CType(DataArray.Item("eventTrigs"), List(Of Object)).Cast(Of String).ToArray()
                Result = UpdateEvent(CStr(DataArray.Item("eventName")), CStr(DataArray.Item("eventDesc")), CBool(DataArray.Item("eventChkEnabled")), CBool(DataArray.Item("eventChkOneOff")), Date.FromBinary(CLng(DataArray.Item("eventFromDateTime"))), Date.FromBinary(CLng(DataArray.Item("eventToDateTime"))), TrigNames, ActionNames)
            Case Is = "PROGRAMS"
                Result = UpdateProgram(CStr(DataArray.Item("programName")), CStr(DataArray.Item("programDesc")), CStr(DataArray.Item("programType")), CStr(DataArray.Item("programLang")), CStr(DataArray.Item("programCode")))
            Case Is = "TRANSFORMS"
                Dim Functions As String() = CType(DataArray.Item("transFuncFunctions"), List(Of Object)).Cast(Of String).ToArray()
                Dim TransformMessage As Structures.HAMessageStruc
                TransformMessage.Network = CByte(DataArray.Item("transFuncOutNetwork"))
                TransformMessage.Category = CStr(DataArray.Item("transFuncOutCategory"))
                TransformMessage.ClassName = CStr(DataArray.Item("transFuncOutClass"))
                TransformMessage.Instance = CStr(DataArray.Item("transFuncOutInstance"))
                TransformMessage.Scope = CStr(DataArray.Item("transFuncOutScope"))
                Result = UpdTransform(CStr(DataArray.Item("TransformName")), CStr(DataArray.Item("TransformDesc")), CBool(DataArray.Item("transFuncOutChk")), CInt(DataArray.Item("transFuncOutRounding")), TransformMessage, Functions)
                If Result = "OK" Then                                   ' Add channel which modifies existing list
                    Dim newCh As New Structures.ChannelStruc
                    newCh.Name = TransformMessage.Instance
                    newCh.Desc = CStr(DataArray.Item("TransformDesc"))
                    newCh.IO = "output"
                    newCh.Units = TransformMessage.Scope
                    newCh.Type = "TRANSFORM"
                    newCh.Value = ""
                    newCh.Attribs = New List(Of Structures.ChannelAttribStruc)
                    HS.modChannel("ADD", HS.GetCatName(CByte(TransformMessage.Category)), TransformMessage.ClassName, newCh)
                End If
        End Select
        Return Result
    End Function

    Public Shared Function ProcessDelMsg(Table As String, RecToDel As String) As String
        Dim Result As String = ""
        Select Case Table.ToUpper
            Case Is = "TRIGGERS"
                Result = DeleteTrigger(RecToDel)
            Case Is = "ACTIONS"
                Result = DeleteAction(RecToDel)
            Case Is = "EVENTS"
                Result = DeleteEvent(RecToDel)
            Case Is = "TRANSFORMS"
                Dim TransRec As DataRow() = GetTransformsInfo(RecToDel)
                Dim delCat As String = HS.GetCatName(CByte(TransRec(0)("Category")))
                Dim delClass As String = CStr(TransRec(0)("Class"))
                Dim delCh As New Structures.ChannelStruc
                delCh.Name = CStr(TransRec(0)("Instance"))
                delCh.Attribs = New List(Of Structures.ChannelAttribStruc)
                Result = DeleteTransform(RecToDel)
                If Result = "OK" Then                                   ' Del new channel
                    HS.modChannel("DEL", delCat, delClass, delCh)
                End If
        End Select
        Return Result
    End Function

#End Region
    '#####################################################################################################################################################################################
#Region "Add Functions"

    ' New event for the automation database 
    Public Shared Function AddNewEvent(EventName As String, EventDescription As String, Enabled As Boolean, OneShot As Boolean, StartDate As DateTime, StopDate As DateTime, TrigNames() As String, ActionNames() As String) As String
        If EventName <> "" Then
            If TrigNames.Count = 0 Then Return "No Event Triggers"
            If ActionNames.Count = 0 Then Return "No Event Actions"
            If GetEventsInfo(EventName).Length > 0 Then
                Return "The entry '" + EventName + "' already exists"
            Else
                Dim NewRow As DataRow = EventsDT.NewRow()
                NewRow.Item("EventName") = EventName
                NewRow.Item("EventDescription") = EventDescription
                NewRow.Item("EventActive") = Enabled
                NewRow.Item("EventOneOff") = OneShot
                NewRow.Item("EventStart") = StartDate.Ticks
                NewRow.Item("EventStop") = StopDate.Ticks
                NewRow.Item("EventLastFired") = 0
                NewRow.Item("EventNumRecur") = 0
                UpdateAutoDB("ADD", "EVENTS", NewRow)

                Dim NewTrigRow As DataRow = Nothing                                             ' Update EventTriggers datatable
                For Lp = 0 To TrigNames.Count - 1
                    NewTrigRow = EventTriggersDT.NewRow()
                    NewTrigRow.Item("EventName") = EventName
                    NewTrigRow.Item("TrigName") = TrigNames(Lp)
                    UpdateAutoDB("ADD", "EVENTTRIGGERS", NewTrigRow)
                Next

                Dim NewActionRow As DataRow = Nothing                                           ' Update EventActions datatable
                For Lp = 0 To ActionNames.Count - 1
                    NewActionRow = EventActionsDT.NewRow()
                    NewActionRow.Item("EventName") = EventName
                    NewActionRow.Item("ActionName") = ActionNames(Lp)
                    UpdateAutoDB("ADD", "EVENTACTIONS", NewActionRow)
                Next

                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "EVENT", "ADDED", EventName, "SYSTEM")
            End If
            Return "OK"
        Else
            Return "No Event name specified"                                                                        ' No Record name specified
        End If
    End Function

    ' Update the automation database with a new action item (errors captured thrown to calling routine)
    Public Shared Function AddNewAction(ActionName As String, ActionDescription As String, Delay As Integer, Random As Boolean, Script As String, ScriptParam As String, Optional HAMessage As Structures.HAMessageStruc = Nothing) As String
        If ActionName <> "" Then
            If GetActionsInfo(ActionName).Length > 0 Then
                Return "The entry '" + ActionName + "' already exists"                                                                    ' The record already exists
            Else
                Dim NewRow As DataRow = ActionsDT.NewRow()
                NewRow.Item("ActionName") = ActionName
                NewRow.Item("ActionDescription") = ActionDescription
                NewRow.Item("ActionScript") = Script
                NewRow.Item("ActionScriptParam") = ScriptParam
                NewRow.Item("ActionDelay") = Delay
                NewRow.Item("ActionRandom") = Random

                NewRow.Item("ActionFunction") = HAMessage.Func
                NewRow.Item("ActionLogLevel") = HAMessage.Level
                NewRow.Item("ActionNetwork") = HAMessage.Network
                NewRow.Item("ActionCategory") = HAMessage.Category
                NewRow.Item("ActionClass") = HAMessage.ClassName
                NewRow.Item("ActionInstance") = HAMessage.Instance
                NewRow.Item("ActionScope") = HAMessage.Scope
                NewRow.Item("ActionData") = HAMessage.Data
                UpdateAutoDB("ADD", "ACTIONS", NewRow)

                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "ACTION", "ADDED", ActionName, "SYSTEM")
            End If
            Return "OK"
        Else
            Return "No Action name specified"                                                                        ' No Record name specified
        End If
    End Function

    ' Update the automation database with a new trigger item
    Public Shared Function AddNewTrigger(TriggerName As String, TriggerDesc As String, Script As String, ScriptParam As String, ScriptCond As Integer, ScriptData As String, ChgCond As Integer,
                                 StateCond As Integer, TrigDateFrom As DateTime, TrigTimeFrom As DateTime, TrigDateTo As DateTime, TrigTimeTo As DateTime, Sunrise As Boolean, Sunset As Boolean, Mon As Boolean, Tue As Boolean, Wed As Boolean,
                                 Thu As Boolean, Fri As Boolean, Sat As Boolean, Sun As Boolean, DayTime As Boolean, NightTime As Boolean, Fortnightly As Boolean,
                                 Monthly As Boolean, Yearly As Boolean, Active As Boolean, Inactive As Boolean, TimeofDay As DateTime, TrigChgMessage As Structures.HAMessageStruc, TrigStateMessage As Structures.HAMessageStruc) As String
        If TriggerName <> "" Then
            If GetTriggersInfo(TriggerName).Length > 0 Then
                Return "The entry '" + TriggerName + "' already exists"
            Else
                Dim NewRow As DataRow = TriggersDT.NewRow()
                NewRow.Item("TrigName") = TriggerName
                NewRow.Item("TrigDescription") = TriggerDesc
                NewRow.Item("TrigScript") = Script
                NewRow.Item("TrigScriptParam") = ScriptParam
                NewRow.Item("TrigScriptCond") = ScriptCond.ToString
                NewRow.Item("TrigScriptData") = ScriptData
                NewRow.Item("TrigStateNetwork") = TrigStateMessage.Network
                NewRow.Item("TrigStateCategory") = TrigStateMessage.Category
                NewRow.Item("TrigStateClass") = TrigStateMessage.ClassName
                NewRow.Item("TrigStateInstance") = TrigStateMessage.Instance
                NewRow.Item("TrigStateScope") = TrigStateMessage.Scope
                NewRow.Item("TrigStateData") = TrigStateMessage.Data
                NewRow.Item("TrigChgNetwork") = TrigChgMessage.Network
                NewRow.Item("TrigChgCategory") = TrigChgMessage.Category
                NewRow.Item("TrigChgClass") = TrigChgMessage.ClassName
                NewRow.Item("TrigChgInstance") = TrigChgMessage.Instance
                NewRow.Item("TrigChgScope") = TrigChgMessage.Scope
                NewRow.Item("TrigChgData") = TrigChgMessage.Data
                NewRow.Item("TrigChgCond") = ChgCond.ToString
                NewRow.Item("TrigStateCond") = StateCond.ToString
                NewRow.Item("TrigDateFrom") = TrigDateFrom.Ticks
                NewRow.Item("TrigTimeFrom") = TrigTimeFrom.Ticks
                NewRow.Item("TrigDateTo") = TrigDateTo.Ticks
                NewRow.Item("TrigTimeTo") = TrigTimeTo.Ticks
                NewRow.Item("TrigFortnightly") = Fortnightly
                NewRow.Item("TrigMonthly") = Monthly
                NewRow.Item("TrigYearly") = Yearly
                NewRow.Item("TrigSunrise") = Sunrise
                NewRow.Item("TrigSunset") = Sunset
                NewRow.Item("TrigDayTime") = DayTime
                NewRow.Item("TrigNightTime") = NightTime
                NewRow.Item("TrigMon") = Mon
                NewRow.Item("TrigTue") = Tue
                NewRow.Item("TrigWed") = Wed
                NewRow.Item("TrigThu") = Thu
                NewRow.Item("TrigFri") = Fri
                NewRow.Item("TrigSat") = Sat
                NewRow.Item("TrigSun") = Sun
                NewRow.Item("TrigActive") = Active
                NewRow.Item("TrigInactive") = Inactive
                NewRow.Item("TrigTimeofDay") = TimeofDay.Ticks
                NewRow.Item("TrigLastFired") = 0
                UpdateAutoDB("ADD", "TRIGGERS", NewRow)
                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "TRIGGERS", "ADDED", TriggerName, "SYSTEM")
            End If
            Return "OK"
        Else
            Return "No Trigger name specified"
        End If
    End Function

    Public Shared Function AddNewTransform(TransformName As String, TransformDesc As String, transFuncOutChk As Boolean, transFuncOutRounding As Integer, TransformMessage As Structures.HAMessageStruc, Functions() As String) As String
        If TransformName <> "" Then
            If Functions.Count = 0 Then Return "No Transform Functions"
            If GetTransformsInfo(TransformName).Length > 0 Then
                Return "The entry '" + TransformName + "' already exists"
            Else
                Dim NewRow As DataRow = TransFuncsDT.NewRow()
                NewRow.Item("TFName") = TransformName
                NewRow.Item("TFDescription") = TransformDesc
                NewRow.Item("Enabled") = transFuncOutChk
                NewRow.Item("RoundDec") = transFuncOutRounding
                NewRow.Item("Network") = TransformMessage.Network
                NewRow.Item("Category") = TransformMessage.Category
                NewRow.Item("Class") = TransformMessage.ClassName
                NewRow.Item("Instance") = TransformMessage.Instance
                NewRow.Item("Scope") = TransformMessage.Scope
                UpdateAutoDB("ADD", "TRANSFUNCS", NewRow)

                AddFuncs(TransformName, Functions)

                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "TRANSFORM", "ADDED", TransformName, "SYSTEM")
            End If
            Return "OK"
        Else
            Return "No Transform name specified"                                                                        ' No Record name specified
        End If
    End Function

    ' Helper function to add new function records
    Private Shared Function AddFuncs(transformName As String, functions As String()) As Boolean
        ' Start Value 22                                                       Starting value a numeric value for the operation (eg. 4.5)
        ' Add Current [SECURITY/CLASS] scope(instance)                         Add current channel value (No time sets)
        ' Subtract Count [SECURITY/CLASS] scope(instance) elapsed(2 minutes)   Subtract count of the channel last two minutes (starting at the start of the minute)
        ' Multipy Average [SECURITY/CLASS] scope(instance) since(time date)    Multipl Average of channel back to the exact time/date
        SyncLock AccessDB
            Dim NewFuncRow As DataRow = Nothing                                             ' Update EventTriggers datatable
            For Lp = 0 To functions.Count - 1
                NewFuncRow = FuncsDT.NewRow()
                Dim SplitFunc() As String = functions(Lp).Split(" "c)
                NewFuncRow.Item("TransFunc") = transformName
                NewFuncRow.Item("FuncType") = SplitFunc(0).Trim()
                NewFuncRow.Item("History") = SplitFunc(1).Trim()
                NewFuncRow.Item("Type") = ""
                NewFuncRow.Item("Prev") = 0
                NewFuncRow.Item("PrevUnit") = ""
                NewFuncRow.Item("PrevFromDate") = 0
                NewFuncRow.Item("PrevFromTime") = 0

                If SplitFunc(1).Trim().ToLower() = "value" Then
                    NewFuncRow.Item("Val") = SplitFunc(2).Trim()
                    NewFuncRow.Item("Network") = 0
                    NewFuncRow.Item("Category") = 0
                    NewFuncRow.Item("Class") = ""
                    NewFuncRow.Item("Instance") = ""
                    NewFuncRow.Item("Scope") = ""
                Else
                    NewFuncRow.Item("Val") = DBNull.Value
                    NewFuncRow.Item("Network") = GlobalVars.myNetNum
                    NewFuncRow.Item("Category") = HS.GetCatNum(functions(Lp).Split("["c)(1).Split("/"c)(0).Trim())
                    NewFuncRow.Item("Class") = functions(Lp).Split("/"c)(1).Split("]"c)(0).Trim()
                    NewFuncRow.Item("Instance") = functions(Lp).Split("]"c)(1).Split("("c)(0).Trim()
                    NewFuncRow.Item("Scope") = functions(Lp).Split("("c)(1).Split(")"c)(0).Trim()
                    If SplitFunc(1).Trim().ToLower() <> "current" Then              ' we have history info, extract from string
                        Dim TypeStr = functions(Lp).Trim().Split(")"c)(1).Split("("c)(0).Trim()
                        NewFuncRow.Item("Type") = TypeStr
                        Dim duration = functions(Lp).Trim().Split(")"c)(1).Split("("c)(1).Trim()
                        If TypeStr.ToLower() = "since" Then
                            Dim myDate = DateTime.Parse(duration.Split(" "c)(0).Trim())
                            NewFuncRow.Item("PrevFromDate") = CLng(DateTime.Parse(duration.Split(" "c)(0).Trim()).Ticks)
                            NewFuncRow.Item("PrevFromTime") = CLng(DateTime.Parse(duration.Split(" "c)(0).Trim()).Ticks)
                        Else
                            NewFuncRow.Item("Prev") = CInt(duration.Split(" "c)(0).Trim())
                            NewFuncRow.Item("PrevUnit") = duration.Split(" "c)(1).Trim()
                        End If
                    End If
                End If
                UpdateAutoDB("ADD", "FUNCS", NewFuncRow)
            Next
        End SyncLock

        Return True
    End Function

#End Region
    '#####################################################################################################################################################################################
#Region "Update Functions"

    ' Update Programs
    Public Shared Function UpdateProgram(Name As String, Desc As String, Type As String, Lang As String, Code As String) As String
        Dim FileName As String
        If Desc = "" Then FileName = Name + "." + Lang Else FileName = Name + "_" + Desc + "." + Lang
        Dim FindFiles() As String = Directory.GetFiles(HS.ClientLocn + "\" + Type + "s", FileName)
        If FindFiles.Length > 0 Then
            Try
                File.Delete(FindFiles(0) + "_old")
                File.Copy(FindFiles(0), FindFiles(0) + "_old")
                File.WriteAllText(FindFiles(0), Code)
                Return "OK"
            Catch ex As Exception
                Return "Error writing file '" + Name + "'. Error: " + ex.ToString
            End Try
        Else
            Return "Program not updated. File '" + Name + "' does not exist"
        End If
    End Function

    ' Update event DB
    Public Shared Function UpdateEvent(EventName As String, EventDescription As String, Enabled As Boolean, OneShot As Boolean, StartDate As DateTime, StopDate As DateTime, TrigNames() As String, ActionNames() As String) As String
        If EventName <> "" Then
            If TrigNames.Count = 0 Then Return "No Event Trigger names specified"
            If ActionNames.Count = 0 Then Return "No Event Action names specified"

            Dim EventRows As DataRow() = GetEventsInfo(EventName)
            If EventRows.Count = 1 Then                                                             ' Should only be 1 record
                Dim EventRowLocn As Integer = EventsDT.Rows.IndexOf(EventRows(0))
                If EventRowLocn = -1 Then Return "Can't locate entry '" + EventName + "' to update"
                SyncLock AccessDB
                    EventsDT(EventRowLocn).Item("EventName") = EventName
                    EventsDT(EventRowLocn).Item("EventDescription") = EventDescription
                    EventsDT(EventRowLocn).Item("EventActive") = Enabled
                    EventsDT(EventRowLocn).Item("EventOneOff") = OneShot
                    EventsDT(EventRowLocn).Item("EventStart") = StartDate.Ticks
                    EventsDT(EventRowLocn).Item("EventStop") = StopDate.Ticks
                End SyncLock
                UpdateAutoDB("UPD", "EVENTS", EventsDT(EventRowLocn))

                Dim RowLocn As Integer
                EventRows = GetEventActionsInfo(EventName)                                          ' Get all the records for the event in the EventActions db
                If EventRows.Count > 0 Then                                                         ' Only delete if there are records (there should be!)
                    SyncLock AccessDB
                        For Each row As DataRow In EventRows                                            ' Can have multiple records for each event name
                            RowLocn = EventActionsDT.Rows.IndexOf(row)
                            If RowLocn <> -1 Then EventActionsDT.Rows(RowLocn).Delete() ' Only delete if I can find the record in the database
                        Next
                    End SyncLock
                    UpdateAutoDB("DEL", "EVENTACTIONS", EventsDT(EventRowLocn))
                End If

                EventRows = GetEventTriggersInfo(EventName)                                         ' Get all the records for the event in the EventTriggers db
                If EventRows.Count > 0 Then                                                         ' Only delete if there are records (there should be!)
                    SyncLock AccessDB
                        For Each row As DataRow In EventRows                                            ' Can have multiple records for each event name
                            RowLocn = EventTriggersDT.Rows.IndexOf(row)
                            If RowLocn <> -1 Then EventTriggersDT.Rows(RowLocn).Delete() ' Only delete if I can find the record in the database
                        Next
                    End SyncLock
                    UpdateAutoDB("DEL", "EVENTTRIGGERS", EventsDT(EventRowLocn))
                End If

                Dim NewTrigRow As DataRow = Nothing                                                 ' Add to EventTriggers datatable
                For Lp = 0 To TrigNames.Count - 1
                    SyncLock AccessDB
                        NewTrigRow = EventTriggersDT.NewRow()
                        NewTrigRow.Item("EventName") = EventName
                        NewTrigRow.Item("TrigName") = TrigNames(Lp)
                    End SyncLock
                    UpdateAutoDB("ADD", "EVENTTRIGGERS", NewTrigRow)
                Next

                Dim NewActionRow As DataRow = Nothing                                               ' Add to EventActions datatable
                For Lp = 0 To ActionNames.Count - 1
                    SyncLock AccessDB
                        NewActionRow = EventActionsDT.NewRow()
                        NewActionRow.Item("EventName") = EventName
                        NewActionRow.Item("ActionName") = ActionNames(Lp)
                    End SyncLock
                    UpdateAutoDB("ADD", "EVENTACTIONS", NewActionRow)
                Next

                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "EVENT", "UPDATED", EventName, "SYSTEM")
            Else
                Return "Database integrity problem, cannot update noexistent Event"
            End If
            Return "OK"
        Else
            Return "No Event name specified"
        End If
    End Function

    ' Update the trigger DB
    Public Shared Function UpdateTrigger(TriggerName As String, TriggerDesc As String, Script As String, ScriptParam As String, ScriptCond As Integer, ScriptData As String, ChgCond As Integer,
                              StateCond As Integer, TrigDateFrom As DateTime, TrigTimeFrom As DateTime, TrigDateTo As DateTime, TrigTimeTo As DateTime, Sunrise As Boolean, Sunset As Boolean, Mon As Boolean, Tue As Boolean, Wed As Boolean,
                              Thu As Boolean, Fri As Boolean, Sat As Boolean, Sun As Boolean, DayTime As Boolean, NightTime As Boolean, Fortnightly As Boolean,
                              Monthly As Boolean, Yearly As Boolean, Active As Boolean, Inactive As Boolean, TimeofDay As DateTime, TrigChgMessage As Structures.HAMessageStruc, TrigStateMessage As Structures.HAMessageStruc) As String
        If TriggerName <> "" Then
            Dim TriggerRows As DataRow() = GetTriggersInfo(TriggerName)
            If TriggerRows.Count = 1 Then                                                           ' Should only be 1 record
                Dim RowLocn As Integer = TriggersDT.Rows.IndexOf(TriggerRows(0))
                If RowLocn = -1 Then Return "Can't locate entry '" + TriggerName + "' to update"
                SyncLock AccessDB
                    TriggersDT(RowLocn).Item("TrigDescription") = TriggerDesc
                    TriggersDT(RowLocn).Item("TrigScript") = Script
                    TriggersDT(RowLocn).Item("TrigScriptParam") = ScriptParam
                    TriggersDT(RowLocn).Item("TrigScriptCond") = ScriptCond.ToString
                    TriggersDT(RowLocn).Item("TrigScriptData") = ScriptData
                    TriggersDT(RowLocn).Item("TrigStateNetwork") = TrigStateMessage.Network
                    TriggersDT(RowLocn).Item("TrigStateCategory") = TrigStateMessage.Category
                    TriggersDT(RowLocn).Item("TrigStateClass") = TrigStateMessage.ClassName
                    TriggersDT(RowLocn).Item("TrigStateInstance") = TrigStateMessage.Instance
                    TriggersDT(RowLocn).Item("TrigStateScope") = TrigStateMessage.Scope
                    TriggersDT(RowLocn).Item("TrigStateData") = TrigStateMessage.Data
                    TriggersDT(RowLocn).Item("TrigChgNetwork") = TrigChgMessage.Network
                    TriggersDT(RowLocn).Item("TrigChgCategory") = TrigChgMessage.Category
                    TriggersDT(RowLocn).Item("TrigChgClass") = TrigChgMessage.ClassName
                    TriggersDT(RowLocn).Item("TrigChgInstance") = TrigChgMessage.Instance
                    TriggersDT(RowLocn).Item("TrigChgScope") = TrigChgMessage.Scope
                    TriggersDT(RowLocn).Item("TrigChgData") = TrigChgMessage.Data
                    TriggersDT(RowLocn).Item("TrigChgCond") = ChgCond.ToString
                    TriggersDT(RowLocn).Item("TrigStateCond") = StateCond.ToString
                    TriggersDT(RowLocn).Item("TrigDateFrom") = TrigDateFrom.Ticks
                    TriggersDT(RowLocn).Item("TrigTimeFrom") = TrigTimeFrom.Ticks
                    TriggersDT(RowLocn).Item("TrigDateTo") = TrigDateTo.Ticks
                    TriggersDT(RowLocn).Item("TrigTimeTo") = TrigTimeTo.Ticks
                    TriggersDT(RowLocn).Item("TrigFortnightly") = Fortnightly
                    TriggersDT(RowLocn).Item("TrigMonthly") = Monthly
                    TriggersDT(RowLocn).Item("TrigYearly") = Yearly
                    TriggersDT(RowLocn).Item("TrigSunrise") = Sunrise
                    TriggersDT(RowLocn).Item("TrigSunset") = Sunset
                    TriggersDT(RowLocn).Item("TrigDayTime") = DayTime
                    TriggersDT(RowLocn).Item("TrigNightTime") = NightTime
                    TriggersDT(RowLocn).Item("TrigMon") = Mon
                    TriggersDT(RowLocn).Item("TrigTue") = Tue
                    TriggersDT(RowLocn).Item("TrigWed") = Wed
                    TriggersDT(RowLocn).Item("TrigThu") = Thu
                    TriggersDT(RowLocn).Item("TrigFri") = Fri
                    TriggersDT(RowLocn).Item("TrigSat") = Sat
                    TriggersDT(RowLocn).Item("TrigSun") = Sun
                    TriggersDT(RowLocn).Item("TrigActive") = Active
                    TriggersDT(RowLocn).Item("TrigInactive") = Inactive
                    TriggersDT(RowLocn).Item("TrigTimeofDay") = TimeofDay.Ticks
                    TriggersDT(RowLocn).Item("TrigLastFired") = 0                  ' Trigger adjusted, so clear last fired field.
                End SyncLock
                UpdateAutoDB("UPD", "TRIGGERS", TriggersDT(RowLocn))

                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "TRIGGERS", "UPDATED", TriggerName, "SYSTEM")
            Else
                Return "Database integrity problem, cannot update noexistent Trigger"
            End If
            Return "OK"
        Else
            Return "No Trigger name specified"
        End If
    End Function

    ' Update actions DB
    Public Shared Function UpdateAction(ActionName As String, ActionDescription As String, Delay As Integer, Random As Boolean, Script As String, ScriptParam As String, Optional HAMessage As Structures.HAMessageStruc = Nothing) As String
        If ActionName <> "" Then
            Dim ActionRows As DataRow() = GetActionsInfo(ActionName)
            If ActionRows.Count = 1 Then                                                            ' Should only be 1 record
                Dim RowLocn As Integer = ActionsDT.Rows.IndexOf(ActionRows(0))
                If RowLocn = -1 Then Return "Can't locate entry '" + ActionName + "' to update"
                SyncLock AccessDB
                    ActionsDT(RowLocn).Item("ActionDescription") = ActionDescription
                    ActionsDT(RowLocn).Item("ActionScript") = Script
                    ActionsDT(RowLocn).Item("ActionScriptParam") = ScriptParam
                    ActionsDT(RowLocn).Item("ActionDelay") = Delay
                    ActionsDT(RowLocn).Item("ActionRandom") = Random

                    ActionsDT(RowLocn).Item("ActionFunction") = HAMessage.Func
                    ActionsDT(RowLocn).Item("ActionLogLevel") = HAMessage.Level
                    ActionsDT(RowLocn).Item("ActionNetwork") = HAMessage.Network
                    ActionsDT(RowLocn).Item("ActionCategory") = HAMessage.Category
                    ActionsDT(RowLocn).Item("ActionClass") = HAMessage.ClassName
                    ActionsDT(RowLocn).Item("ActionInstance") = HAMessage.Instance
                    ActionsDT(RowLocn).Item("ActionScope") = HAMessage.Scope
                    ActionsDT(RowLocn).Item("ActionData") = HAMessage.Data
                End SyncLock
                UpdateAutoDB("UPD", "ACTIONS", ActionsDT(RowLocn))

                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "ACTIONS", "UPDATED", ActionName, "SYSTEM")
            Else
                Return "Database integrity problem, cannot update noexistent Action"
            End If
            Return "OK"
        Else
            Return "No Action name specified"                                                                            ' No Record name specified
        End If
    End Function

    Public Shared Function UpdTransform(TransformName As String, TransformDesc As String, transFuncOutChk As Boolean, transFuncOutRounding As Integer, TransformMessage As Structures.HAMessageStruc, Functions() As String) As String
        If TransformName <> "" Then
            If Functions.Count = 0 Then Return "No Transform Functions"
            Dim TransformRows As DataRow() = GetTransformsInfo(TransformName)
            If TransformRows.Count = 1 Then

                Dim TransformRowLocn As Integer = TransFuncsDT.Rows.IndexOf(TransformRows(0))
                If TransformRowLocn = -1 Then Return "Can't locate entry '" + TransformName + "' to update"
                SyncLock AccessDB
                    TransFuncsDT(TransformRowLocn).Item("TFName") = TransformName
                    TransFuncsDT(TransformRowLocn).Item("TFDescription") = TransformDesc
                    TransFuncsDT(TransformRowLocn).Item("Enabled") = transFuncOutChk
                    TransFuncsDT(TransformRowLocn).Item("RoundDec") = transFuncOutRounding
                    TransFuncsDT(TransformRowLocn).Item("Network") = TransformMessage.Network
                    TransFuncsDT(TransformRowLocn).Item("Category") = TransformMessage.Category
                    TransFuncsDT(TransformRowLocn).Item("Class") = TransformMessage.ClassName
                    TransFuncsDT(TransformRowLocn).Item("Instance") = TransformMessage.Instance
                    TransFuncsDT(TransformRowLocn).Item("Scope") = TransformMessage.Scope
                End SyncLock
                UpdateAutoDB("UPD", "TRANSFUNCS", TransFuncsDT(TransformRowLocn))

                Dim RowLocn As Integer
                Dim FuncRows = GetFunctionsInfo(TransformName)                                          ' Get all the records for the tranform in the Funcs db
                If FuncRows.Count > 0 Then                                                         ' Only delete if there are records (there should be!)
                    SyncLock AccessDB
                        For Each row As DataRow In FuncRows                                            ' Can have multiple records for each transform name
                            RowLocn = FuncsDT.Rows.IndexOf(row)
                            If RowLocn <> -1 Then FuncsDT.Rows(RowLocn).Delete() ' Only delete if I can find the record in the database
                        Next
                    End SyncLock
                    UpdateAutoDB("DEL", "FUNCS", TransformRows(0))              ' Use the TransFunc record which has TFName column needed for delete
                End If

                AddFuncs(TransformName, Functions)

                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "TRANSFORM", "UPDATED", TransformName, "SYSTEM")
                Return "OK"
            Else
                Return "Database integrity problem, cannot update noexistent Transform"
            End If
        Else
            Return "No Transform name specified"                                                                        ' No Record name specified
        End If
    End Function

#End Region
    '#####################################################################################################################################################################################
#Region "Delete Functions"
    ' Delete event from the events table & DB as well as any records with the eventname in the eventactions and eventtriggers tables & DB
    Public Shared Function DeleteEvent(EventName As String) As String
        If EventName <> "" Then
            Dim FindRows As DataRow() = GetEventsInfo(EventName)                                     ' Find the records with the Event name
            If FindRows.Count = 1 Then                                                              ' There should only be 1 record returned
                Dim EventRowLocn As Integer = EventsDT.Rows.IndexOf(FindRows(0))
                If EventRowLocn = -1 Then Return "Can't locate entry '" + EventName + "' to delete"

                Dim RowLocn As Integer
                FindRows = GetEventActionsInfo(EventName)                                           ' Get all the records for the event in the EventActions db
                If FindRows.Count > 0 Then                                                          ' Only delete if there are records (there should be!)
                    SyncLock AccessDB
                        For Each row As DataRow In FindRows                                         ' Can have multiple records for each event name
                            RowLocn = EventActionsDT.Rows.IndexOf(row)
                            If RowLocn <> -1 Then EventActionsDT.Rows(RowLocn).Delete() ' Only delete if I can find the record in the database
                        Next
                    End SyncLock
                    UpdateAutoDB("DEL", "EVENTACTIONS", EventsDT.Rows(EventRowLocn))                ' Delete eventactions record. Note EventsDT datatable is used for the eventname as it has not been deleted yet
                End If

                FindRows = GetEventTriggersInfo(EventName)                                          ' Get all the records for the event in the EventTriggers db
                If FindRows.Count > 0 Then                                                          ' Only delete if there are records (there should be!)
                    SyncLock AccessDB
                        For Each row As DataRow In FindRows                                         ' Can have multiple records for each event name
                            RowLocn = EventTriggersDT.Rows.IndexOf(row)
                            If RowLocn <> -1 Then EventTriggersDT.Rows(RowLocn).Delete() ' Only delete if I can find the record in the database
                        Next
                    End SyncLock
                    UpdateAutoDB("DEL", "EVENTTRIGGERS", EventsDT.Rows(EventRowLocn))               ' Delete eventtriggers record. Note EventsDT datatable is used for the eventname as it has not been deleted yet
                End If

                UpdateAutoDB("DEL", "EVENTS", EventsDT.Rows(EventRowLocn), EventRowLocn)            ' Delete the event at the end as the event information is needed to delete eventtriggers & eventactions

                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "EVENT", "DELETED", EventName, "SYSTEM")
                Return "OK"
            End If
        End If
        Return "No Event name specified"
    End Function

    ' Delete ActionName from the datatable and the actions database
    Public Shared Function DeleteAction(ActionName As String) As String
        If ActionName <> "" Then
            If EventActionsDT.Select("ActionName='" + ActionName + "'").Count > 0 Then Return "Can't locate entry '" + ActionName + "' to update" ' Can't delete if there is a dependent record in events table
            Dim FindRows As DataRow() = GetActionsInfo(ActionName)                                   ' Find the records with the Action name
            If FindRows.Count = 1 Then                                                              ' There should only be 1 record returned
                Dim RowLocn As Integer = ActionsDT.Rows.IndexOf(FindRows(0))
                If RowLocn = -1 Then Return "Can't locate entry '" + ActionName + "' to delete"
                UpdateAutoDB("DEL", "ACTIONS", ActionsDT.Rows(RowLocn), RowLocn)

                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "ACTION", "DELETED", ActionName, "SYSTEM")
                Return "OK"
            End If
        End If
        Return "No Action name specified"
    End Function

    ' Delete Trigger from the database and trigger datatable
    Public Shared Function DeleteTrigger(TriggerName As String) As String
        If TriggerName <> "" Then
            If EventTriggersDT.Select("TrigName='" + TriggerName + "'").Count > 0 Then Return "Can't locate entry '" + TriggerName + "' to update" ' Can't delete if there is a dependent record in events table
            Dim FindRows As DataRow() = GetTriggersInfo(TriggerName)                                     ' Find the records with the trigger name
            If FindRows.Count = 1 Then                                                                  ' There should only be 1 record returned
                Dim RowLocn As Integer = TriggersDT.Rows.IndexOf(FindRows(0))
                If RowLocn = -1 Then Return "Can't locate entry '" + TriggerName + "' to delete"
                UpdateAutoDB("DEL", "TRIGGERS", TriggersDT.Rows(RowLocn), RowLocn)

                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "TRIGGER", "DELETED", TriggerName, "SYSTEM")
                Return "OK"
            End If
        End If
        Return "No Trigger name specified"
    End Function

    Public Shared Function DeleteTransform(TransformName As String) As String
        If TransformName <> "" Then
            Dim FindRows As DataRow() = GetTransformsInfo(TransformName)                                     ' Find the records with the Transform name
            If FindRows.Count = 1 Then                                                              ' There should only be 1 record returned
                Dim TransformRowLocn As Integer = TransFuncsDT.Rows.IndexOf(FindRows(0))
                If TransformRowLocn = -1 Then Return "Can't locate entry '" + TransformName + "' to delete"

                Dim RowLocn As Integer
                FindRows = GetFunctionsInfo(TransformName)                                           ' Get all the records for the Transform in the Funcs db
                If FindRows.Count > 0 Then                                                          ' Only delete if there are records (there should be!)
                    SyncLock AccessDB
                        For Each row As DataRow In FindRows                                         ' Can have multiple records for each Transform name
                            RowLocn = FuncsDT.Rows.IndexOf(row)
                            If RowLocn <> -1 Then FuncsDT.Rows(RowLocn).Delete() ' Only delete if I can find the record in the database
                        Next
                    End SyncLock
                    UpdateAutoDB("DEL", "FUNCS", TransFuncsDT.Rows(TransformRowLocn))                ' Delete Transformactions record. Note TransformsDT datatable is used for the Transformname as it has not been deleted yet
                End If

                UpdateAutoDB("DEL", "TRANSFUNCS", TransFuncsDT.Rows(TransformRowLocn), TransformRowLocn)            ' Delete the Transform at the end as the Transform information is needed to delete Transformtriggers & Transformactions

                HS.CreateMessage("AUTOMATION", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "TRANSFORM", "DELETED", TransformName, "SYSTEM")
                Return "OK"
            End If
        End If
        Return "No Transform name specified"
    End Function

#End Region
    '#####################################################################################################################################################################################
#Region "DB Utilities"
    ' Build the SQLite command string for inserts. Only handles integers, boolean and strings
    Public Shared Function SQLiteInsertCommandBuilder(Table As String, Row As DataRow) As String
        Dim SQLStr As String = "INSERT INTO " + Table + " ("
        For Lp = 0 To Row.ItemArray.Count - 1                                                           ' Build the column name fields
            SQLStr = SQLStr + Row.Table.Columns(Lp).ColumnName + ", "
        Next
        SQLStr = SQLStr.Substring(0, SQLStr.Length - 2) + ") VALUES ("
        For Lp = 0 To Row.ItemArray.Count - 1                                                           ' Build the column values
            If (TypeOf Row.Item(Lp) Is Boolean) Then
                If DirectCast(Row.Item(Lp), Boolean) = True Then SQLStr = SQLStr + "1"c Else SQLStr = SQLStr + "0"c ' Booleans stored as binary 1 or 0
            ElseIf (TypeOf Row.Item(Lp) Is String) Then                                                 ' Strings have quotes
                SQLStr = SQLStr + "'"c
                SQLStr = SQLStr + Row.Item(Lp).ToString
                SQLStr = SQLStr + "'"c
            Else
                If IsDBNull(Row.Item(Lp)) Then
                    SQLStr = SQLStr + "NULL"                                                            ' SQLite Null
                Else
                    SQLStr = SQLStr + Row.Item(Lp).ToString                                             ' Integers dont have quotes
                End If
                ' TODO - UPDATE FOR DATE TYPES
            End If
            SQLStr = SQLStr + ", "
        Next
        Return SQLStr.Substring(0, SQLStr.Length - 2) + ")"c                                             ' Remove the last comma
    End Function

    ' Build the SQLite command string for updates. Only handles integers, boolean and strings. RecordName is in the format <ColumnName> = '<ColumnValue>'
    Public Shared Function SQLiteUpdateCommandBuilder(Table As String, WhereClause As String, Row As DataRow) As String
        Dim SQLStr As String = "UPDATE " + Table + " SET "
        For Lp = 0 To Row.ItemArray.Count - 1                                                           ' Iterate through all column names and values in the datarow
            SQLStr = SQLStr + Row.Table.Columns(Lp).ColumnName + "="c
            If (TypeOf Row.Item(Lp) Is Boolean) Then
                If DirectCast(Row.Item(Lp), Boolean) = True Then SQLStr = SQLStr + "1"c Else SQLStr = SQLStr + "0"c ' Booleans stored as binary 1 or 0
            ElseIf (TypeOf Row.Item(Lp) Is String) Then
                SQLStr = SQLStr + "'"c
                SQLStr = SQLStr + Row.Item(Lp).ToString                                                 ' Strings have quotes
                SQLStr = SQLStr + "'"c
            Else
                If IsDBNull(Row.Item(Lp)) Then
                    SQLStr = SQLStr + "NULL"                                                            ' SQLite Null
                Else
                    SQLStr = SQLStr + Row.Item(Lp).ToString                                             ' Integers dont have quotes
                End If
                ' TODO - UPDATE FOR DATE TYPES
            End If
            SQLStr = SQLStr + ", "
        Next
        Return SQLStr.Substring(0, SQLStr.Length - 2) + " WHERE " + WhereClause                         ' Remove the last comma
    End Function

    ' Thread safe updates to datatable and SQLite automation DB
    Public Shared Function UpdateAutoDB(Func As String, Table As String, Row As DataRow, Optional RowLocn As Integer = 0) As Boolean
        SyncLock AccessDB                           ' Lock this method to avoid concurrent thread access
            Select Case Table.ToUpper
                Case Is = "TRIGGERS"
                    If Func.ToUpper = "ADD" Then TriggersSQLCmd.CommandText = SQLiteInsertCommandBuilder("Triggers", Row)
                    If Func.ToUpper = "UPD" Then TriggersSQLCmd.CommandText = SQLiteUpdateCommandBuilder("Triggers", "TrigName='" + CStr(Row.Item("TrigName")) + "'"c, Row)
                    If Func.ToUpper = "DEL" Then TriggersSQLCmd.CommandText = "DELETE FROM Triggers WHERE TrigName = '" + CStr(Row.Item("TrigName")) + "'"c
                    TriggersSQLCmd.ExecuteNonQuery()
                    If Func.ToUpper = "DEL" Then TriggersDT.Rows(RowLocn).Delete()
                    If Func.ToUpper = "ADD" Then TriggersDT.Rows.Add(Row) 'myTable.ImportRow(dr);
                    'If Func.ToUpper = "ADD" Then TriggersDT.ImportRow(Row) 'myTable.ImportRow(dr);
                    TriggersDT.AcceptChanges()
                Case Is = "EVENTS"
                    If Func.ToUpper = "ADD" Then EventsSQLCmd.CommandText = SQLiteInsertCommandBuilder("Events", Row)
                    If Func.ToUpper = "UPD" Then EventsSQLCmd.CommandText = SQLiteUpdateCommandBuilder("Events", "EventName='" + CStr(Row.Item("EventName")) + "'"c, Row)
                    If Func.ToUpper = "DEL" Then EventsSQLCmd.CommandText = "DELETE FROM Events WHERE EventName = '" + CStr(Row.Item("EventName")) + "'"c
                    EventsSQLCmd.ExecuteNonQuery()
                    If Func.ToUpper = "DEL" Then EventsDT.Rows(RowLocn).Delete()
                    If Func.ToUpper = "ADD" Then EventsDT.Rows.Add(Row)
                    EventsDT.AcceptChanges()
                Case Is = "ACTIONS"
                    If Func.ToUpper = "DEL" Then ActionsSQLCmd.CommandText = "DELETE FROM Actions WHERE ActionName = '" + CStr(Row.Item("ActionName")) + "'"c
                    If Func.ToUpper = "ADD" Then ActionsSQLCmd.CommandText = SQLiteInsertCommandBuilder("Actions", Row)
                    If Func.ToUpper = "UPD" Then ActionsSQLCmd.CommandText = SQLiteUpdateCommandBuilder("Actions", "ActionName='" + CStr(Row.Item("ActionName")) + "'"c, Row)
                    ActionsSQLCmd.ExecuteNonQuery()
                    If Func.ToUpper = "DEL" Then ActionsDT.Rows(RowLocn).Delete()
                    If Func.ToUpper = "ADD" Then ActionsDT.Rows.Add(Row)
                    ActionsDT.AcceptChanges()
                Case Is = "EVENTACTIONS"
                    If Func.ToUpper = "DEL" Then EventActionsSQLCmd.CommandText = "DELETE FROM EventActions WHERE EventName = '" + CStr(Row.Item("EventName")) + "'"c
                    If Func.ToUpper = "ADD" Then EventActionsSQLCmd.CommandText = SQLiteInsertCommandBuilder("EventActions", Row)
                    EventActionsSQLCmd.ExecuteNonQuery()
                    If Func.ToUpper = "ADD" Then EventActionsDT.Rows.Add(Row)
                    EventActionsDT.AcceptChanges()
                Case Is = "EVENTTRIGGERS"
                    If Func.ToUpper = "DEL" Then EventTriggersSQLCmd.CommandText = "DELETE FROM EventTriggers WHERE EventName = '" + CStr(Row.Item("EventName")) + "'"c
                    If Func.ToUpper = "ADD" Then EventTriggersSQLCmd.CommandText = SQLiteInsertCommandBuilder("EventTriggers", Row)
                    EventTriggersSQLCmd.ExecuteNonQuery()
                    If Func.ToUpper = "ADD" Then EventTriggersDT.Rows.Add(Row)
                    EventTriggersDT.AcceptChanges()
                Case Is = "TRANSFUNCS"
                    If Func.ToUpper = "ADD" Then TransFuncsSQLCmd.CommandText = SQLiteInsertCommandBuilder("TransFuncs", Row)
                    If Func.ToUpper = "UPD" Then TransFuncsSQLCmd.CommandText = SQLiteUpdateCommandBuilder("TransFuncs", "TFName='" + CStr(Row.Item("TFName")) + "'"c, Row)
                    If Func.ToUpper = "DEL" Then TransFuncsSQLCmd.CommandText = "DELETE FROM TransFuncs WHERE TFName = '" + CStr(Row.Item("TFName")) + "'"c
                    TransFuncsSQLCmd.ExecuteNonQuery()
                    If Func.ToUpper = "DEL" Then TransFuncsDT.Rows(RowLocn).Delete()
                    If Func.ToUpper = "ADD" Then TransFuncsDT.Rows.Add(Row)
                    TransFuncsDT.AcceptChanges()
                Case Is = "FUNCS"
                    If Func.ToUpper = "DEL" Then FuncsSQLCmd.CommandText = "DELETE FROM Funcs WHERE TransFunc = '" + CStr(Row.Item("TFName")) + "'"c
                    If Func.ToUpper = "ADD" Then FuncsSQLCmd.CommandText = SQLiteInsertCommandBuilder("Funcs", Row)
                    FuncsSQLCmd.ExecuteNonQuery()
                    If Func.ToUpper = "ADD" Then FuncsDT.Rows.Add(Row)
                    FuncsDT.AcceptChanges()
            End Select
        End SyncLock
        Return True
    End Function
#End Region
End Class

