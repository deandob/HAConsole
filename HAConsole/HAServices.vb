' Library Imports (add references to the the bin\lib directory)
'Imports Alchemy                 ' Websockets library
Imports System.Data.SQLite                  ' SQLite RDBMS
Imports Commons                 ' Common interfaces and structures for HA Server & plugins

' CLR namespaces
Imports System.IO
Imports System.Text
Imports System.Reflection
'Imports System.Net.WebSockets
Imports System.Threading
Imports System.Net
Imports System.Security
Imports System.Collections.Concurrent
Imports HAConsole.HAUtils
Imports fastJSON
Imports System.Text.RegularExpressions

Namespace HAServices

    Public Class HomeServices
        Inherits MarshalByRefObject                                     ' Serialize calls
        Implements Plugins.IHAServer

        Private DebugSW As New Stopwatch                                        ' Stopwatch counter for debug & performance profiling

        Public WithEvents NodeProc As New Process

        Private LogConn As New ArrayList
        Private SQLCmd As New SQLiteCommand                   ' Command for insert SQL (reused with parameters)

        Private LogDT As New DataTable                            ' Load console from the log file

        Private DBLock As New Object                                         ' Lock to ensure no concurrent reading / writing to the database
        Private ChannelLock As New Object                          ' Lock to ensure no concurrent reading / writing
        Private LogLock As New Object

        Private PauseDB As Boolean = False                                  ' Stop the DB message queue while changing DB parameters (eg. reloading the console)

        Private LogCreated As Date                                     ' When the log was created

        Private OldTime As Date = DateTime.Now                            ' Used by the timer to see if minutes, hours, days etc. have gone past

        Private AllMessages As New Structures.HAMessageStruc                    ' No message, used for processing triggers (returns true when testing for event messages) to ignore message checks when testing for time based triggers

        Public Shared ClientLogs As String = ""                                ' Global used to send console logs to clients

        Private ServiceState As Integer = HAConst.ServiceState.PAUSE    ' Server state variable, initially paused (taking messages but not processing)

        Private Event LogConsole(LogMessage As Structures.HAMessageStruc)    ' Event needed to pass message back to UI thread
        Private Event MessageEvent(EventMgs As Structures.HAMessageStruc)

        Private MessageBQ As New Concurrent.BlockingCollection(Of Structures.HAMessageStruc)      ' Blocking async queue for messages
        Private DBActionBQ As New Concurrent.BlockingCollection(Of Structures.HAMessageStruc)      ' Blocking async queue for processing DB requests

        Public HAStateStore As New ConcurrentDictionary(Of Structures.StateStoreKey, String)       ' State store for persisting state of the data field for messages. Keys are NETWORK.CATEGORY.CLASSNAME.INSTANCE.SCOPE where NETWORK and CATEGORY are text

        Private myPlugin As Plugins.IHAPlugin = Nothing
        Private Plugins As New ConcurrentDictionary(Of String, Structures.PlugStruc)
        Private PluginsStarted As Boolean = False                     ' Flag for plugins loaded or not
        Private IniMsgs As New List(Of String)                    ' Temporary message store for messages sent by plugins during initialisation but before plugin framework is ready

        ' Global Classes used
        Public Ini As IniFile                                       ' Ini file management
        Public ClientIni As IniFile
        Private HAScripts As HAUtils.Scripting                                          ' Object that keeps the context of all the scripts in the script directory
        Public IsLinux As Boolean                                   ' Linux or Windows
        Public DataDir As String                                    ' Platform agnostic data file location in the user documents directory

        ' Read only static properties loaded from the ini file
        'Property LogFile As String                                          ' File name and location of console logs (table name = file name)
        Property DBLocn As String
        Property NodePluginMgrLocn As String                                     ' Location of Node.JS plugin Manager
        Property myNetName As String
        Property TimerTick As Integer
        Property Scriptpath As String                                            ' Directory of the script files
        Property URLFindLongLat As String                                 ' URL help string to get long / lat degrees/min for a city/town
        Property URLGetLongLat As String                                                     ' URL to automatically get Long / Lat by IP address
        Property ClientLocn As String

        ' Read only properties modified by this routine but available outside
        Property SunRiseTime As DateTime
        Property SunSetTime As DateTime
        Property SolarNoonTime As DateTime
        Property IsNight As Boolean                                         ' Nightime (always opposite of IsDay)
        Property IsDay As Boolean                                           ' DayTime (always opposite of IsNight)
        Property IsSunset As Boolean                                        ' IsSunset stays active +- the sunrise/set offset (use the sunset event if looking for a state change)
        Property IsSunrise As Boolean                                       ' Similar to IsSunset, and each of the IsXXXX is exclusive, only one of them will be set

        ' Public variables that can be set by outside functions usually UI
        Public ArchiveFreq As String                             ' How often to create an archive
        Public ViewLogFrom As Integer                               ' Number of months back to view the message logs in the console
        Public Latitude As String
        Public Longitude As String
        Public SunriseSetOffset As Integer                     ' +- offset from sunrise & sunset to trigger sunrise / sunset flags
        Public EnableTimer As Boolean                         ' Enable / disable the system timer

        ' Public constants (not adjustable)
        Public Const LOG_EXT As String = ".DB3"                              ' Log file extension name
        Public Const ERROR_LOG_FILE_NAME As String = "SystemErrors.log"      ' Text file log of system errors that can't be logged to the database
        Public Const DATA_DIR_NAME As String = "HAData"                      ' Relative to home directory
        Public Const WEB_CLIENT_DIR_NAME As String = "HAWebClient"           ' Relative to exe location
        Public Const NODEJS_DIR_NAME As String = "PluginMgr"                 ' Relative to exe location

        'TODO: Archiving

#Region "Initialization"

        Public Sub Init()
            Try
                IsLinux = (Environment.OSVersion.Platform = 4) Or (Environment.OSVersion.Platform = 6) Or (Environment.OSVersion.Platform = 128)
                DataDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Personal), DATA_DIR_NAME)
                If Not File.Exists(Path.Combine(DataDir, "settings.ini")) Then
                    Directory.CreateDirectory(DataDir)
                    File.Copy(Path.Combine({Environment.CurrentDirectory, "..", "..", "settings.template"}), Path.Combine(DataDir, "settings.ini"))
                    File.Copy(Path.Combine({Environment.CurrentDirectory, "..", "..", "clients.template"}), Path.Combine(DataDir, "clients.ini"))
                    WriteConsole(True, "First time setup. Data directory, clients.ini and settings.ini created in " + DataDir + ". Edit settings.ini in this directory to change server settings.")
                End If

                WriteConsole(True, "Starting automation services...")

                Ini = New IniFile(Path.Combine(DataDir, "settings"))                                                           ' Open and cache the ini file settings
                ClientIni = New IniFile(Path.Combine(DataDir, "clients"))
                'ClientLocn = ClientIni.Get("general", "FileLocn", DataDir)
                ClientLocn = ClientIni.Get("general", "FileLocn", Path.Combine({Environment.CurrentDirectory, "..", "..", "..", "..", WEB_CLIENT_DIR_NAME}))
                If ClientLocn = "" Then ClientLocn = Path.Combine({Environment.CurrentDirectory, "..", "..", "..", "..", WEB_CLIENT_DIR_NAME})
                'LogFile = Ini.Get("database", "MessLogName", "MessLog")                                    ' name of message log file
                DBLocn = Ini.Get("database", "DBPath", DataDir)                                               ' Location to store log files
                If DBLocn = "" Then DBLocn = DataDir
                NodePluginMgrLocn = ClientIni.Get("nodepluginmgr", "NodePluginMgrLocn", Path.Combine({Environment.CurrentDirectory, "..", "..", "..", "..", NODEJS_DIR_NAME, NODEJS_DIR_NAME}))                                                 ' Location of node.js plugin manager
                If NodePluginMgrLocn = "" Then NodePluginMgrLocn = Path.Combine({Environment.CurrentDirectory, "..", "..", "..", "..", NODEJS_DIR_NAME, NODEJS_DIR_NAME})
                If Not File.Exists(Path.Combine(NodePluginMgrLocn, "nodebat.bat")) Then
                    HandleSysErrors(True, "ERROR", "NODE.JS Plugin Manager files not found at " + NodePluginMgrLocn + ". Install Plugin Manager files before continuing. Exiting", New Exception("No Node Plugin Manager files found"))
                End If

                ArchiveFreq = Ini.Get("database", "Archive", "Monthly")                                       ' Frequency of creating archive logs
                GlobalVars.myNetName = Ini.Get("Server", "NetworkName", "Home").ToUpper                                        ' Network name of server
                TimerTick = CInt(Ini.Get("console", "TimerTick", "1000"))                                    ' msec timer ticks
                ViewLogFrom = CInt(Ini.Get("console", "ViewLogFrom", "3"))                          ' Get the view window time for the log console
                Latitude = Ini.Get("system", "Latitude", "0.0")                                    ' To calculate sunrise / sunset
                Longitude = Ini.Get("system", "Longitude", "0.0")                                    ' To calculate sunrise / sunset
                SunriseSetOffset = CInt(Ini.Get("system", "SunriseSetOffset", "30"))                 ' Offset +- mins to set sunrise / sunset flags
                Scriptpath = Ini.Get("scripts", "scriptpath", "scripts")
                If Scriptpath = "" Then Scriptpath = "scripts"

                URLFindLongLat = Ini.Get("system", "URLFindLongLat", "http://www.getty.edu/research/tools/vocabularies/tgn")  ' Help link to calculate Long/Lat based on location
                URLGetLongLat = Ini.Get("system", "URLGetLongLat", "http://www.geobytes.com/IpLocator.htm?GetLocation&Template=XML.txt")   ' Finds Long/Lat via IP address

                Dim ListItem As String                                                                                      ' Cache all the valid category names
                Dim myCat As Structures.CatStruc
                myCat.Cat = "ALL"
                myCat.Icon = ""
                GlobalVars.CategoryColl.Add(myCat)                                                                           ' Internal category representing all categories (for trigger matching) Constant CATEGORY_ALL
                myCat.Cat = "SYSTEM"
                myCat.Icon = "wrench"
                GlobalVars.CategoryColl.Add(myCat)                                                                           ' Internal system category Constant CatConsts.SYSTEM
                For Lp = 0 To 255
                    ListItem = Ini.Get("Categories", "Cat" + Lp.ToString, "").ToUpper
                    If ListItem = "" Then Exit For
                    myCat.Cat = ListItem
                    myCat.Icon = Ini.Get("CatIcons", ListItem, "")
                    GlobalVars.CategoryColl.Add(myCat)
                Next
                GlobalVars.NetworkColl.Add("ALL")                                                                           ' Internal network representing all networks (for trigger matching) Constant NETWORK_ALL
                For Lp As Byte = 0 To 255                                                                                               ' Cache all the valid networks
                    ListItem = Ini.Get("Networks", "Network" + Lp.ToString, "").ToUpper
                    If ListItem = "" Then Exit For
                    GlobalVars.NetworkColl.Add(ListItem)
                    If ListItem.ToUpper = GlobalVars.myNetName.ToUpper Then GlobalVars.myNetNum = Lp + CByte(1)
                Next

                With AllMessages
                    .Network = 0                  ' All networks
                    .Category = "ALL"                ' All categories
                    .ClassName = ""
                    .Instance = ""
                    .Scope = ""
                    .Data = ""
                End With

                CalcSunRiseSet()                                                                        ' Work out sunrise sunset
                SetTimeOfDay()                                                                          ' Set the time of day flags
                StartMsgQueue()
                InitLogDB()
                CompileScripts()
                StartTimerThread()                                                                                              ' Dedicated thread for timed events
                LoadPlugins(Path.Combine(Directory.GetCurrentDirectory(), "Plugins"))                               ' Load all valid plugins on a separate thread as plugins can update the UI before the form is fully rendered
                Automation.InitAutoDB()                                                                 ' Initialise the automation DB & engine
                LoadNodePlugins()

            Catch ex As Exception
                HandleSysErrors(True, "ERROR", "Fatal error in starting automation services. Exiting...", ex)
            End Try
        End Sub

        Private Function LoadNodePlugins() As Boolean
            Dim oldNodeProc As New List(Of Process)
            For Each process As Process In Process.GetProcesses()
                If process.ProcessName.Contains("node") Then
                    WriteConsole(True, "Node.JS already running, killing...")
                    process.Kill()
                End If
            Next
            NodeProc.StartInfo.FileName = "nodebat.bat"
            NodeProc.StartInfo.Arguments = "plugmgr.js " + DebugMode.ToString()
            NodeProc.StartInfo.WorkingDirectory = NodePluginMgrLocn
            NodeProc.EnableRaisingEvents = True
            WriteConsole(True, "Starting Node.JS pluginMgr...")
            NodeProc.Start()
            WriteConsole(True, "Started Node.JS pluginMgr...")
            Return True
        End Function

        Private Sub NodeProcess_HasExited(ByVal sender As Object, ByVal e As System.EventArgs) Handles NodeProc.Exited
            WriteConsole(True, "Node.JS console exited.")
            If NodeProc.ExitCode > 0 Then                                    ' Don't capture normal termination
                WriteConsole(True, "WARNING: Node process aborted (error code " + NodeProc.ExitCode.ToString() + "). Restarting")
                System.Threading.Thread.Sleep(10000)
                If NodeProc.ExitCode = 99 Then
                    System.Diagnostics.Process.Start("shutdown.exe", "-r -f -t 0")  ' reboot
                Else
                    LoadNodePlugins()                                               ' restart
                End If
            End If
        End Sub

        ' Compile all the scripts in the Scripts directory ready for ServiceState.RUNNING
        Private Sub CompileScripts()
            HAScripts = New Scripting
            For Each ScriptFile In HAUtils.GetScriptFileNames()
                WriteConsole(True, "Loading script '" + ScriptFile + "'")
                If HAScripts.CompileScriptFile(ScriptFile, "") = False Then
                    Dim ex As New Exception
                    HandleSysErrors(False, "WARNING", "Cannot compile script '" + ScriptFile + "'. Error: " + HAScripts.CompileErr, ex)
                End If
            Next
        End Sub

        ' Run a script with parameters
        Public Function RunScript(ScriptName As String, Optional param1 As String = Nothing, Optional param2 As String = Nothing, Optional param3 As String = Nothing, Optional param4 As String = Nothing) As String
            Return HAScripts.Run(ScriptName, param1, param2, param3, param4)
        End Function

        ' Start the timer processing thread
        Private Function StartTimerThread() As Boolean
            Try
                Dim TimerThread As New Thread(AddressOf TimerLoop)
                TimerThread.IsBackground = True
                TimerThread.Start()
            Catch ex As Exception
                Throw
                Return False
            End Try
            Return True
        End Function

        ' Start the DB queue processing thread
        Private Function StartDBQueue() As Boolean
            Try
                Dim DBThread As New Thread(AddressOf ProcessDBActions)
                DBThread.IsBackground = True
                DBThread.Start()
            Catch ex As Exception
                Throw
                Return False
            End Try
            Return True
        End Function

        ' Start up the Message queue processing thread 
        Private Function StartMsgQueue() As Boolean
            Try
                Dim BrokerThread As New Thread(AddressOf HAMessageBroker)                   ' Start the message broker thread
                BrokerThread.IsBackground = True
                BrokerThread.Start()
                StartServer()                                                               ' Open up the message queue
                Return True
            Catch ex As Exception
                Throw
                Return False
            End Try
        End Function

#End Region
        '##############################################################################################################################################################################
#Region "Log Database"

        ' Open the database and load the datatable and datagridview
        ' Database performance notes: This datamodel is pretty efficient on the disk, 1M records should use up about 50Mb, which is about 1 years worth of data
        ' Data adaptor fill is initially pretty slow, takes about 10 seconds to load 1M records on a fast machine with a SSD. About 400MB RAM for 1M records
        Private Sub InitLogDB()
            Try
                For Lp = 0 To GlobalVars.CategoryColl.Count - 2             ' ignore ALL category
                    Dim myLogConn As New SQLite.SQLiteConnection
                    myLogConn.ConnectionString = "Data Source=" + Path.Combine(DBLocn, CType(GlobalVars.CategoryColl.Item(Lp + 1), Structures.CatStruc).Cat.ToString) + LOG_EXT + ";"
                    If Not File.Exists(Path.Combine(DBLocn, CType(GlobalVars.CategoryColl.Item(Lp + 1), Structures.CatStruc).Cat.ToString) + LOG_EXT) Then                    ' If no log file exists then create it
                        myLogConn.Open()                                              ' Creates datafile if one does not exist (so has to execute after we check for data file existence)
                        SQLCmd = myLogConn.CreateCommand
                        If Environment.OSVersion.Platform = PlatformID.Win32NT Then             ' Tune the file size for the operating system (can set this in the ini file)
                            SQLCmd.CommandText = "PRAGMA page_size=" + Ini.Get("System", "WindowsClusterSize", "4096") + ";"          ' Windows
                        Else
                            SQLCmd.CommandText = "PRAGMA page_size=" + Ini.Get("System", "LinuxClusterSize", "1024") + ";"          ' Linux
                        End If
                        SQLCmd.ExecuteNonQuery()
                        SQLCmd.CommandText = "CREATE TABLE MESSLOG (ID INTEGER PRIMARY KEY AUTOINCREMENT, TIME INTEGER, CLASS TEXT COLLATE NOCASE, INSTANCE TEXT COLLATE NOCASE, SCOPE TEXT COLLATE NOCASE, DATA TEXT COLLATE NOCASE);"
                        SQLCmd.ExecuteNonQuery()
                        WriteConsole(True, "Creating log database for category: " + GetCatName(CByte(Lp + 1)) + "...")
                    Else
                        myLogConn.Open()                                              ' Open existing log
                        SQLCmd = myLogConn.CreateCommand
                        WriteConsole(True, "Opening log database for category: " + GetCatName(CByte(Lp + 1)) + "...")
                    End If
                    LogConn.Add(myLogConn)
                Next


                'SQLCmd.CommandText = "CREATE TABLE MESSLOG (ID INTEGER PRIMARY KEY AUTOINCREMENT, TIME INTEGER, FUNC INTEGER, LEVEL INTEGER, CLASS TEXT COLLATE NOCASE, INSTANCE TEXT COLLATE NOCASE, SCOPE TEXT COLLATE NOCASE, DATA TEXT COLLATE NOCASE);"
                'Dim transaction = CType(LogConn.Item(xx), SQLite.SQLiteConnection).BeginTransaction()
                'Dim cmdText As String
                'Dim cmd As SQLiteCommand
                'Dim xlp = 0
                'For Each row As DataRow In dt.Rows
                'cmdText = "INSERT INTO MESSLOG (time, func, level, class, instance, scope, data) VALUES (" + row("TIME").ToString + ", " + row("FUNC").ToString + ", " + row("LEVEL").ToString + ", '" + row("CLASS").ToString + "', '" + row("INSTANCE").ToString + "', '" + row("SCOPE").ToString + "', '" + row("DATA").ToString + "');"
                'cmdText = "INSERT INTO MESSLOG (time, class, instance, scope, data) VALUES (@a, @d, @e, @f, @g);"
                'cmd = New SQLiteCommand(cmdText, CType(LogConn.Item(xx), SQLite.SQLiteConnection), transaction)
                'cmd.CommandType = CommandType.Text
                'cmd.Parameters.Add(New SQLiteParameter("@a", row("TIME").ToString))
                'cmd.Parameters.Add(New SQLiteParameter("@b", row("FUNC").ToString))
                'cmd.Parameters.Add(New SQLiteParameter("@c", row("LEVEL").ToString))
                'cmd.Parameters.Add(New SQLiteParameter("@d", row("CLASS").ToString))
                'cmd.Parameters.Add(New SQLiteParameter("@e", row("INSTANCE").ToString))
                'cmd.Parameters.Add(New SQLiteParameter("@f", row("SCOPE").ToString))
                ' cmd.Parameters.Add(New SQLiteParameter("@g", row("DATA").ToString))
                'cmd.ExecuteNonQuery()
                'Console.Write(xlp.ToString + vbCrLf)
                'xlp = xlp + 1
                'Next
                'ransaction.Commit()

                LogCreated = File.GetCreationTime(Path.Combine(DBLocn, "SYSTEM" + LOG_EXT))                  ' Used to determine if its time to archive the logs

                StartDBQueue()                                                                  ' Start the action queue for handling the database

            Catch ex As Exception
                HandleSysErrors(True, "ERROR", "Fatal error in loading console log data. Exiting...", ex)
            End Try
        End Sub

        ' Prepare record for submitting to the database
        Public Function ExecuteDB(CommandMsg As Structures.HAMessageStruc) As Boolean
            Try
                If CommandMsg.Data.Contains("'"c) Then CommandMsg.Data = CommandMsg.Data.Replace("'"c, "''") ' Can't save single quotes as it upsets SQL syntax, convert to double ''
                DBActionBQ.Add(CommandMsg) ' add messages to the DB message queue for actioning
                Return True
            Catch ex As Exception
                HandleSysErrors(True, "ERROR", "Cannot update message log database. Exiting...", ex)
                Return False
            End Try
        End Function

        ' THREAD: Process all the DB actions on the DB action message queue. Use a message queue to ensure single thread access to the database
        Private Sub ProcessDBActions()
            Try
                'Dim rx As New Regex("[^\u0020-\u007F]")
                For Each DBMsg As Structures.HAMessageStruc In DBActionBQ.GetConsumingEnumerable
                    Try
                        Do While PauseDB = True                                     ' If the DB is paused wait until it is free before processing more actions for DB
                            Thread.Sleep(1)
                        Loop
                        ' XXXXXXXXXXX NEED TO PROTECT FROM SQL INJECTION 
                        SyncLock DBLock                                             ' As log additions are async, ensure no concurrency problems by adding a critical section
                            If DBMsg.Scope.Length > 3 AndAlso DBMsg.Scope.Substring(0, 2).ToUpper = "DB" Then           ' DB Function called
                                If DBMsg.Data.Length > 6 Then
                                    Select Case DBMsg.Data.Substring(0, 6).ToUpper
                                        Case Is = "SELECT"                                  ' Run a SQL query
                                            Dim dt As DataTable = New DataTable()
                                            Dim da As SQLite.SQLiteDataAdapter = New SQLite.SQLiteDataAdapter
                                            SQLCmd.CommandText = DBMsg.Data
                                            da.SelectCommand = SQLCmd
                                            dt.BeginLoadData()
                                            da.Fill(dt)                                     ' Load the datatable with the result of the query
                                            dt.EndLoadData()
                                            '''Dim JSONparam As New JSONParameters
                                            '''JSONparam.UseExtensions = False                 ' Don't bother with the JSON schema extension, assume the calling routine understands the resultset context / Types
                                            '''Dim JSONStr As String = JSON.Instance.ToJSON(dt, JSONparam)         ' Convert the datatable records to JSON
                                            '''CreateMessage(DBMsg.ClassName, HAConst.FUNC_RSP, DBMsg.Level, DBMsg.Instance, DBMsg.Scope, JSONStr, DBMsg.Category)         ' Send the response back to the requestor with the data payload. Specify the calling category so it can be routed back to the caller
                                        Case Is = "UPDATE"
                                    End Select
                                Else
                                    HandleAppErrors(HAConst.MessErr.MEDIUM, DBMsg.Category, DBMsg.ClassName, DBMsg.Instance, DBMsg.Scope, "DB Command not long enough, ignoring command string: " + DBMsg.Data)
                                End If
                            End If
                            If DBMsg.Level > HAConst.MessLog.NONE And DBMsg.Category <> "ALL" Then              ' Don't log when log level is NONE
                                SQLCmd = CType(LogConn.Item(GetCatNum(DBMsg.Category) - 1), SQLite.SQLiteConnection).CreateCommand
                                'MsgBox(Date.UtcNow.ToString + " (" + Date.UtcNow.ToBinary.ToString + " " + Date.UtcNow.Ticks.ToString + "  --- " + New Date(DBMsg.Time.Ticks).ToString + " + " + DBMsg.Time.Ticks.ToString + " " + DBMsg.Time.ToBinary.ToString)
                                'SQLCmd.CommandText = "INSERT INTO MessLog (time, func, level, network, category, class, instance, scope, data) VALUES (" + Date.UtcNow.Ticks.ToString + ", " + DBMsg.Func.ToString + ", " + DBMsg.Level.ToString + ", " + DBMsg.Network.ToString + ", " + GetCatNum(DBMsg.Category).ToString + ", '" + DBMsg.ClassName + "', '" + DBMsg.Instance + "', '" + DBMsg.Scope + "', '" + DBMsg.Data + "')"

                                ' TODO: For class, scope, instance only accept letters and numbers (a \ will cause a crash)                             
                                SQLCmd.CommandText = "INSERT INTO MessLog (time, class, instance, scope, data) VALUES (" + Date.UtcNow.Ticks.ToString + ", '" + DBMsg.ClassName + "', '" + DBMsg.Instance + "', '" + DBMsg.Scope + "', '" + DBMsg.Data + "')"
                                'SQLCmd.CommandText = "INSERT INTO MessLog (time, class, instance, scope, data) VALUES (" + Date.UtcNow.Ticks.ToString + ", '" + Encoding.ASCII.GetString(Encoding.ASCII.GetBytes(DBMsg.ClassName)) + "', '" + Encoding.ASCII.GetString(Encoding.ASCII.GetBytes(DBMsg.Instance)) + "', '" + Encoding.ASCII.GetString(Encoding.ASCII.GetBytes(DBMsg.Scope)) + "', '" + Encoding.ASCII.GetString(Encoding.ASCII.GetBytes(DBMsg.Data)) + "')"       ' Strip any non ASCII characters that can cause a crash
                                'SQLCmd.CommandText = "INSERT INTO MessLog (time, class, instance, scope, data) VALUES (" + Date.UtcNow.Ticks.ToString + ", '" + StripStr(DBMsg.ClassName) + "', '" + StripStr(DBMsg.Instance) + "', '" + StripStr(DBMsg.Scope) + "', '" + StripStr(DBMsg.Data) + "')"
                                SQLCmd.ExecuteNonQuery()
                            End If
                        End SyncLock

                    Catch ex As Exception
                        HandleSysErrors(False, "WARNING", "Cannot update message log database, continuing but skipping this transaction: " + SQLCmd.CommandText, ex)
                    End Try
                Next
            Catch ex As Exception
                HandleSysErrors(True, "ERROR", "Cannot access message log queue. Exiting...", ex)
            End Try
        End Sub

        ' Save the current log to an archive file and start a fresh log file. Called from the time routine based on ArchiveFreq
        Public Function ArchiveLog() As Boolean
            Try
                '  SubmitMessage(ConsoleMessage)
                '''Console.StopServices(False)                                  ' Temporarily stop the server
                '''LogDA.Dispose()
                'LogConn.Close()                                 ' Close the log file
                'My.Computer.FileSystem.MoveFile(DBLocn + "\"c + LogFile + LOG_EXT, DBLocn + "\"c + LogFile + DateTime.Now.ToString("_dd-MM-yy") + LOG_EXT, True)         ' Move the temporary file to the ini file
                'InitLogDB()
                '''Console.StartServices(False)                                 ' Start back up again
                'CreateMessage("Server", HAConst.MessFunc.LOG, HAConst.MessLog.NORMAL, "Services", "LogFile", "Log archived to " + DateTime.Now.Date.ToString("dd-MM-yy_") + LogFile + LOG_EXT)
            Catch ex As Exception
                HandleSysErrors(False, "WARNING", "Cannot archive the log file. ", ex)
                Return False
            End Try
            Return True
        End Function

        ' Extract records from the logs database
        Public Function GetLogs(cols As String, filter As String, CatNum As Integer, OrderLimit As String) As DataRow()
            If cols = "" Then cols = "*"
            SyncLock DBLock
                SQLCmd = CType(LogConn.Item(CatNum), SQLite.SQLiteConnection).CreateCommand         ' Select appropriate catalog log file
                PauseDB = True              ' Stop processing logs, keep them in the queue
                SQLCmd.CommandText = "SELECT " + cols.ToUpper + " FROM MessLog WHERE " + filter + " ORDER BY " + OrderLimit  ' sort by time ASC / DESC
                Dim dt As DataTable = New DataTable()
                Dim da As SQLite.SQLiteDataAdapter = New SQLite.SQLiteDataAdapter
                da.SelectCommand = SQLCmd
                dt.BeginLoadData()
                da.Fill(dt)                                     ' Load the datatable with the result of the query
                dt.EndLoadData()
                Dim datarows As DataRow() = dt.Select("")
                PauseDB = False
                dt.Dispose()
                Return datarows
            End SyncLock
        End Function

#End Region
        '##############################################################################################################################################################################
#Region "Message Management"

        ' THREAD: Look for items on the message bus queue, blocking if there is nothing
        Private Sub HAMessageBroker()
            For Each HAMessage As Structures.HAMessageStruc In MessageBQ.GetConsumingEnumerable
                'MsgBox("Processed " + HAMessage.Data)
                Do While ServiceState <> HAConst.ServiceState.RUNNING
                    Thread.Sleep(10)                                                            ' If the service has stopped or paused, block don't process the message queue
                Loop
                ThreadPool.QueueUserWorkItem(AddressOf HandleMessage, HAMessage)
                If HAMessage.Level <> HAConst.MessLog.NONE Then RaiseEvent LogConsole(HAMessage) ' Log onto the console & file (begininvoke used so no waiting time)
            Next
        End Sub

        ' Parse client commands in form CMD1:CMD2(ReturnFunction), returning individual parameters in string array
        Private Function ParseCmd(myCmd As String) As String()
            Dim Params(2) As String
            myCmd = myCmd.ToUpper
            Dim Delimiter As Integer = myCmd.IndexOf(":"c)
            If Delimiter > 0 And Delimiter < (myCmd.Length - 1) Then                                    ' formatted correctly?
                Params(0) = myCmd.Substring(0, Delimiter)                                               ' 1st cmd
                Dim TempStr = myCmd.Substring(Delimiter + 1)                                              ' 2nd CMD
                Delimiter = TempStr.IndexOf("("c)                                                     ' If there is a returning function, extract that too.
                If Delimiter > 0 And Delimiter < (TempStr.Length - 1) Then
                    Params(1) = TempStr.Substring(0, Delimiter)                                       ' Remove the (
                    Params(2) = TempStr.Substring(Delimiter + 1, TempStr.Length - Params(1).Length - 2).ToUpper                    ' Callback routine on the client
                Else
                    Params(2) = Nothing                                                                 ' No brackets, so pass nothing back for callback
                End If
                Return Params                                                                           ' Return string array
            Else
                Return Nothing                                                                          ' Incorrect format, pass nothing back
            End If
        End Function

        ' THREAD: Handle the message created from the message queue
        Private Sub HandleMessage(Message As Object)
            Dim myMessage As Structures.HAMessageStruc = DirectCast(Message, Structures.HAMessageStruc)
            Try
                Select Case myMessage.Func
                    Case Is = HAConst.MessFunc.ACTION
                        Select Case myMessage.Category
                            Case "SYSTEM"
                                Select Case myMessage.ClassName.ToUpper
                                    Case Is = "HISTORY"
                                        Dim GetColl As System.Collections.Generic.IEnumerable(Of Object) = ProcessGetHistory(myMessage.Scope, CLng(myMessage.Data.Split(","c)(0)), CLng(myMessage.Data.Split(","c)(1)))      ' Send message back to client with data in datatable
                                        HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, GetColl)      ' Send message back to client with data in datatable
                                    Case Is = "QUERY"   ' send("QUERY", cat, type +":" + className + "(" + conditions + ")", channel)
                                        Dim GetColl As System.Collections.Generic.IEnumerable(Of Object) = ProcessGetQuery(myMessage.Scope, myMessage.Data)      ' Send message back to client with data in datatable
                                        HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, GetColl)      ' Send message back to client with data in datatable
                                    Case Is = "CONSOLE"
                                        Select Case myMessage.Data.ToUpper
                                            Case Is = "START"
                                                ClientLogs = myMessage.Instance.ToUpper                 ' Global used when writing console logs to send to client named
                                            Case Is = "STOP"
                                                ClientLogs = ""
                                            Case Is = "START DEBUG"
                                                If ClientLogs <> "" Then HAConsole.DebugMode = True
                                            Case Is = "STOP DEBUG"
                                                If ClientLogs <> "" Then HAConsole.DebugMode = False
                                            Case Is = "STATESTORE"
                                                If ClientLogs <> "" Then HAConsole.ListStateStore()
                                        End Select
                                    Case Is = "SETTINGS"
                                        Dim myParams As String() = ParseCmd(myMessage.Scope)
                                        If myParams IsNot Nothing Then
                                            Select Case myParams(0).ToUpper                     ' 1st command
                                                Case Is = "GET"
                                                    HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, ProcessGetMsg(myParams(1), myMessage.Data))      ' Send message back to client with data in datatable
                                                Case Is = "PUT"
                                                    HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, ProcessPutMsg(myParams(1), myMessage.Data))      ' Send message back to client with data in datatable
                                                Case Is = "UPD"
                                                Case Is = "DEL"
                                            End Select
                                        End If
                                    Case Is = "SCREENS"
                                        Select Case myMessage.Scope
                                            Case Is = "LOAD"
                                                HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, ClientIni.Get("Desktop", myMessage.Instance, ""))
                                            Case Is = "SAVE"
                                                ClientIni.Set("Desktop", myMessage.Instance, myMessage.Data)
                                                ClientIni.WriteIniSettings()
                                        End Select
                                    Case Is = "WIDGETS"
                                        Select Case myMessage.Scope
                                            Case Is = "LOAD"
                                                HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, ClientIni.Get("Widgets", myMessage.Instance, ""))
                                            Case Is = "SAVE"
                                                ClientIni.Set("Widgets", myMessage.Instance, myMessage.Data)
                                                ClientIni.WriteIniSettings()
                                            Case Is = "INISUB"
                                                IniSubscribe(myMessage.Instance, myMessage)
                                            Case Is = "TOOLBOX"
                                                HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, GetProgramNames("WIDGETS"))
                                            Case Is = "CHANNELS"
                                                SendChannelNames(myMessage.Instance, myMessage)
                                            Case Is = "FILE"                   ' download file is in the format base64 with directory, widget number, filename, image attributes appended and separated by comma
                                                Dim splitFile() = myMessage.Data.Split(","c)            ' filename appended to start of base64 string with ; delimiter
                                                Select Case splitFile(0).Trim
                                                    Case Is = "IMAGE"             ' images
                                                        Select Case splitFile(3)
                                                            Case Is = "data:image/png;base64", "data:image/bmp;base64", "data:image/jpeg;base64"             ' images
                                                                Dim ImgLoc As String = Path.Combine(ClientLocn, "images")
                                                                Dim img As System.Drawing.Image
                                                                Dim fileBytes As Byte() = Convert.FromBase64String(splitFile(4))
                                                                Using ms As MemoryStream = New MemoryStream
                                                                    ms.Write(fileBytes, 0, fileBytes.Length)
                                                                    img = System.Drawing.Image.FromStream(ms)
                                                                    img.Save(Path.Combine(ImgLoc, splitFile(2)))
                                                                End Using
                                                                img.Dispose()
                                                                myMessage.Scope = "IMAGE"
                                                                HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.EVENT, myMessage, splitFile(1))     ' Ask widget to reload image
                                                        End Select
                                                End Select
                                                splitFile = Nothing
                                            Case Else
                                        End Select
                                    Case Is = "AUTOMATION"
                                        Dim myParams As String() = ParseCmd(myMessage.Scope)
                                        If myParams IsNot Nothing Then
                                            Select Case myParams(0).ToUpper                     ' 1st command
                                                Case Is = "GET"
                                                    Dim GetColl As System.Collections.Generic.IEnumerable(Of Object) = Automation.ProcessGetMsg(myParams(1), myMessage.Data)
                                                    If IsNothing(GetColl) Then
                                                        HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, Nothing)      ' Send message back to client with empty datatable (as CopyToDataTable errors if no rows)
                                                    Else
                                                        If GetColl.Count = 0 Then
                                                            HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, Nothing)      ' Send message back to client with empty datatable (as CopyToDataTable errors if no rows)
                                                        Else
                                                            HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, GetColl)      ' Send message back to client with data in datatable
                                                        End If
                                                    End If
                                                Case Is = "ADD"
                                                    HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, Automation.ProcessAddMsg(myParams(1), CType(fastJSON.JSON.Parse(myMessage.Data), System.Collections.Generic.List(Of Object))))
                                                Case Is = "UPD"
                                                    HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, Automation.ProcessUpdMsg(myParams(1), CType(fastJSON.JSON.Parse(myMessage.Data), System.Collections.Generic.List(Of Object))))
                                                Case Is = "DEL"
                                                    HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.RESPONSE, myMessage, Automation.ProcessDelMsg(myParams(1), myMessage.Data))
                                            End Select
                                        End If
                                End Select
                        End Select

                    Case Is = HAConst.MessFunc.RESPONSE                                                                                   ' Assume response messages are going to plugins
                        SendtoPlugin(myMessage)
                    Case Is = HAConst.MessFunc.ERROR                                                                                   ' Handle errors raised
                    Case Is = HAConst.MessFunc.LOG                                                                                   ' Log to logs (no action needed, will log automatically)
                    Case Is = HAConst.MessFunc.EVENT                                                                                  ' Event raised
                        If myMessage.Category <> "SYSTEM" And myMessage.ClassName <> "TIME" Then
                            'TODO: Should this be an object instance 'as new automation'?
                            Automation.AutomationEvents(myMessage)                                                                           ' Process automation events by checking triggers (invoked by all events except timer, timer 1 min checks will also run this routine)
                            Automation.Transform(myMessage)                                                                           ' Process transform events to create new events based on saved transforms
                            SendtoPlugin(myMessage)                                                                                     ' Plugins can action if needed
                            'SetStoreState(myMessage)                                                                                    ' Update the state store
                            SendtoSubscribers(myMessage)
                            WriteConsole(False, "Cat:" + myMessage.Category.ToString + " Class:" + myMessage.ClassName + " Instance:" + myMessage.Instance + " Scope:" + myMessage.Scope + " Data:" + myMessage.Data)
                        Else                                                                                                        ' Timer events
                            Select Case myMessage.Instance.ToUpper
                                Case Is = "YEAR"
                                    ProcessYearChange()
                                Case Is = "MONTH"
                                    ProcessMonthChange()
                                Case Is = "WEEK"
                                    ProcessWeekChange()
                                Case Is = "DAY"
                                    ProcessDayChange()
                                Case Is = "HOUR"
                                    ProcessHourChange()
                                Case Is = "MINUTE"
                                    ProcessMinuteChange(myMessage)
                                Case Is = "SECOND"
                                    ProcessSecondChange()
                                Case Is = "TICK"
                                    ProcessTickChange()
                            End Select

                        End If
                End Select
                RaiseEvent MessageEvent(myMessage)                                                                                  ' Send out the message to all subscribers to Message Event
            Catch ex As Exception                                                                                                   ' Top level error handling for all message management, errors will be thrown back to this routine
                HandleAppErrors(HAConst.MessLog.MAJOR, "SYSTEM", "MESSAGEMANAGEMENT", "HandleMessage", "ERROR", "Error handling message", ex, myMessage)           ' Write to system error log, but continue
            End Try
        End Sub

        Private Function SendChannelNames(user As String, clientMessage As Structures.HAMessageStruc) As Boolean
            'Dim PlugChs As New Dictionary(Of String, Structures.PlugStruc)
            'Dim tempPlug As New Structures.PlugStruc
            'For Each Plugin As KeyValuePair(Of String, Structures.PlugStruc) In Plugins
            ' tempPlug.Category = Plugin.Value.Category
            ' tempPlug.ClassName = Plugin.Value.ClassName
            ' tempPlug.Channels = Plugin.Value.Channels
            ' PlugChs.Add(Plugin.Key, tempPlug)
            ' Next
            Dim JSONChannels As String = fastJSON.JSON.ToJSON(Plugins)
            'Dim JSONChannels As String = fastJSON.JSON.ToJSON(PlugChs)          ' Temporary object as JSON serialisation can't deal with references inside object
            HomeNet.SendClient(user, HAConst.MessFunc.RESPONSE, clientMessage, JSONChannels)
        End Function

        Public Function GetProgramNames(type As String) As String
            Dim ProgramDir As String
            If type = "WIDGETS" Then ProgramDir = Path.Combine(ClientLocn, type) Else ProgramDir = type
            Dim di As New DirectoryInfo(ProgramDir)
            Dim fileNames As String = ""
            Dim SearchExts As String() = {}
            Select Case type
                Case Is = "WIDGETS"
                    SearchExts = {"*.svg", "*.html"}
                Case Is = "SCRIPTS"
                    SearchExts = {"*.vb", "*.cs", "*.scr"}
                Case Is = "PLUGINS"
                    SearchExts = {"*.vb", "*.cs"}
            End Select
            If di.Exists Then
                For Each ext As String In SearchExts
                    For Each program As FileInfo In di.GetFiles(ext)
                        If fileNames <> "" Then
                            If program.Name.IndexOf("_old") = -1 Then fileNames = fileNames + "," + program.Name
                        Else
                            If program.Name.IndexOf("_old") = -1 Then fileNames = program.Name
                        End If
                    Next
                Next
            End If
            Return fileNames
        End Function

        ' Called by client for bulk subscribe to channels & return all relevant initial cached state information
        Private Function IniSubscribe(user As String, clientMessage As Structures.HAMessageStruc) As Boolean
            Try
                Dim SubChs() As String = clientMessage.Data.Split(","c)             ' Extract channel subscription requests delimited by commas
                Dim ClassInst() As String
                Dim FoundKeys As IDictionary(Of Structures.StateStoreKey, String)
                Dim channelList As New Dictionary(Of String, String)                ' response format in list of (scope:data)

                Dim ChState As Structures.HAMessageStruc
                ChState.Network = GetNetNum(myNetName)
                ChState.Category = clientMessage.Category
                ChState.ClassName = clientMessage.ClassName
                ChState.Instance = clientMessage.Instance
                ChState.Scope = ""    ' All

                For Each SubCh In SubChs                                            ' Search statestore for all relevant information for the instance
                    ClassInst = SubCh.Split("/"c)                                   ' category\class\instance = category\plugin\channel
                    If SubCh <> "" Then
                        If Not HomeNet.HAClients(user).Subscribed.Contains(SubCh) Then HomeNet.HAClients(user).Subscribed.Add(SubCh) ' Only 1 unique channel subscription per client
                        ChState.Category = ClassInst(0)                             ' Get the latest state info for the INI response
                        ChState.ClassName = ClassInst(1)
                        ChState.Instance = ClassInst(2)
                        FoundKeys = SearchStoreStates(ChState)                      ' Find all the cached state information for the plugin instance
                        For Each kvp In FoundKeys
                            If kvp.Key.Scope.Substring(0, 1) <> "_" Then channelList(SubCh) = kvp.Key.Scope + ":" + kvp.Value ' ignore system messages starting with _
                        Next
                    End If
                Next

                Return HomeNet.SendClient(clientMessage.Instance, HAConst.MessFunc.RESPONSE, clientMessage, channelList)


            Catch ex As Exception
                Dim t = 1

            End Try
        End Function

        Private Function SendtoSubscribers(myMessage As Structures.HAMessageStruc) As Boolean

            Dim NetMessage As Structures.HAMessageStruc = myMessage

            For Each client As KeyValuePair(Of String, HANetwork.NetServices.ClientStruc) In HomeNet.HAClients
                If Not IsNothing(client.Value.Subscribed) Then
                    For Each Subscription As String In client.Value.Subscribed
                        If myMessage.Scope.Length = 0 Then              ' Don't send messages starting with _ (system messages)
                            If Subscription.ToUpper = myMessage.Category.ToUpper + "/" + myMessage.ClassName.ToUpper + "/" + myMessage.Instance.ToUpper Then
                                HomeNet.MsgQSend(client.Key, HAConst.MessFunc.EVENT, NetMessage, myMessage.Data)          ' Send client the full message
                            End If
                        Else
                            If myMessage.Scope.Substring(0, 1) <> "_" And Subscription.ToUpper = myMessage.Category.ToUpper + "/" + myMessage.ClassName.ToUpper + "/" + myMessage.Instance.ToUpper Then
                                HomeNet.MsgQSend(client.Key, HAConst.MessFunc.EVENT, NetMessage, myMessage.Data)          ' Send client the full message
                            End If
                        End If
                    Next
                End If
            Next

        End Function

        Private Function ProcessPutMsg(Func As String, Data As String) As Boolean
            Select Case Func.ToUpper
                Case Is = "INI"
                    Return SetIniSettings(Data)
            End Select
        End Function

        Public Function ProcessGetMsg(Func As String, Data As String) As Array
            Dim DummyArray As Array = Nothing
            Select Case Func.ToUpper
                Case Is = "CATEGORIES"
                    Return GlobalVars.CategoryColl.ToArray
                Case Is = "NETWORKS"
                    Return GlobalVars.NetworkColl.ToArray
                Case Is = "INI"
                    Return GetIniSettings(Data)
                Case Is = "VALUE"
                    Dim DataSplit = Data.Split("/"c)
                    Return GetStoreState(New Structures.HAMessageStruc With {.Network = GlobalVars.myNetNum, .Category = DataSplit(0), .ClassName = DataSplit(1), .Instance = DataSplit(2), .Scope = DataSplit(3)}).ToArray
            End Select
            Return DummyArray                             ' Nothing selected
        End Function

        ' Get query result from logs
        Private Function ProcessGetQuery(Cmd As String, Data As String) As List(Of Object)
            'TODO: Consolidate with ProcessGetHistory which is a subset of this.
            'type:category/class/instance(TIMEFRAME[start,finish])
            Dim CmdSplit = Cmd.Split(":"c)
            Dim CatSplit = CmdSplit(1).Split("/"c)
            Dim InstanceSplit = CatSplit(2).Split("("c)
            Dim TimeFrameSplit = InstanceSplit(1).Split("["c)
            Dim StartFinishSplit = TimeFrameSplit(1).Split("]"c)(0).Split(","c)
            Dim start = CLng(StartFinishSplit(0))
            Dim finish = CLng(StartFinishSplit(1))
            Dim StartTime As Long = HAUtils.FromJSTIme(start)                              ' Convert Javascript time to WIndows Time
            Dim FinishTime As Long = HAUtils.FromJSTIme(finish)
            Dim Datatest = Data.Split("|"c)                                     ' Data in format <test>|<data> where test is a string 'greater' 'equal' 'less' etc.
            Dim GetColl As New List(Of Object)
            Dim ResTest As Integer = HAConst.TestCond.EQUALS
            Select Case Datatest(0).ToUpper()
                Case Is = "EQUALS"
                    ResTest = HAConst.TestCond.EQUALS
                Case Is = "GREATER"
                    ResTest = HAConst.TestCond.GREATER_THAN
                Case Is = "LESS"
                    ResTest = HAConst.TestCond.LESS_THAN
                Case Is = "NOT"
                    ResTest = HAConst.TestCond.NOT_EQUAL
                Case Is = "CONTAINS"
                    ResTest = HAConst.TestCond.CONTAINS
                Case Is = "CHANGE"
                    ResTest = HAConst.TestCond.CHANGE
            End Select
            Dim CondStr = ""
            Dim ResMsg As New Commons.Structures.HAMessageStruc
            Select Case CmdSplit(0)
                Case Is = "INSTANCE"
                    If TimeFrameSplit(0).ToUpper = "CURRENT" Then
                        Dim SearchMsg As New Structures.HAMessageStruc With {.Network = GlobalVars.myNetNum, .Category = CatSplit(0), .ClassName = CatSplit(1), .Instance = InstanceSplit(0), .Scope = "", .Data = Datatest(1)}
                        Dim RetSearch = SearchStoreStates(SearchMsg)

                        For Each KeyMsg As KeyValuePair(Of Structures.StateStoreKey, String) In RetSearch
                            With ResMsg
                                .Time = Date.UtcNow
                                .Network = GetNetNum(KeyMsg.Key.Network)
                                .Category = KeyMsg.Key.Category
                                .ClassName = KeyMsg.Key.ClassName
                                .Instance = KeyMsg.Key.Instance
                                .Scope = KeyMsg.Key.Scope
                                .Data = KeyMsg.Value
                            End With
                            If Datatest(1) = "" Or Automation.TestData(ResTest, Datatest(1), KeyMsg.Value) Then GetColl.Add(ResMsg)
                        Next
                        Return GetColl
                    Else
                        'CondStr = "CLASS='" + ClassSplit(1) + "' AND INSTANCE='" + Instance + "' AND TIME BETWEEN " + StartTime.ToString() + " AND " + FinishTime.ToString()
                    End If
                Case Is = "TIME"
                Case Is = "VALUE"
            End Select

            Dim dt As DataTable = New DataTable()
            Dim da As SQLite.SQLiteDataAdapter = New SQLite.SQLiteDataAdapter
            ' Get all matching records in the timerange
            Dim getRows As DataRow() = GetLogs("TIME,CLASS,INSTANCE,SCOPE,DATA", CondStr, GetCatNum(CatSplit(0)) - 1, "TIME DESC")   ' **** CATER FOR 'ALL' CAT
            If IsNothing(getRows) Then Return Nothing

            ' Can't compress dataset as time will be unique for each record
            For Each rowNum In getRows
                GetColl.Add(rowNum(0).ToString() + "," + rowNum(1).ToString())
            Next
            'For rowNum = 0 To getRows.Length - 1
            'If rowNum <> 0 And rowNum <> (getRows.Length - 1) Then              'only add if the previous or next record is different to compress dataset
            'If getRows(rowNum)(1).ToString() <> getRows(rowNum - 1)(1).ToString() Or getRows(rowNum)(1).ToString() <> getRows(rowNum + 1)(1).ToString() Then GetColl.Add(getRows(rowNum)(0).ToString() + "," + getRows(rowNum)(1).ToString())
            'Else
            'GetColl.Add(getRows(rowNum)(0).ToString() + "," + getRows(rowNum)(1).ToString())
            'End If
            'Next
            'Dim record, oldY As String
            'For Each dr As DataRow In getRows
            '    record = dr(0).ToString + "," + dr(1).ToString
            '    If oldY <> dr(1).ToString Then GetColl.Add(record)
            '    oldY = dr(1).ToString
            '    'Dim DictRow As Dictionary(Of String, Object) = dr.Table.Columns.Cast(Of DataColumn)().ToDictionary(Function(col) col.ColumnName, Function(col) dr.Field(Of Object)(col.ColumnName))
            '    'GetColl.Add(DictRow)
            'Next
            Return GetColl
        End Function

        ' Get history array of previous channel data from logs
        Private Function ProcessGetHistory(channel As String, start As Long, finish As Long) As List(Of Object)
            Dim ChSplit = channel.Split("/"c)
            Dim dt As DataTable = New DataTable()
            Dim da As SQLite.SQLiteDataAdapter = New SQLite.SQLiteDataAdapter
            ' Get all matching records in the timerange

            Dim StartTime As Long = HAUtils.FromJSTIme(start)                              ' Convert Javascript time to WIndows Time
            Dim FinishTime As Long = HAUtils.FromJSTIme(finish)
            'Dim CondStr As String = "CATEGORY=" + CStr(GetCatNum(ChSplit(0))) + " AND CLASS='" + ChSplit(1) + "' AND INSTANCE='" + ChSplit(2) + "' AND TIME BETWEEN " + StartTime.ToString() + " AND " + FinishTime.ToString()
            Dim CondStr As String = "CLASS='" + ChSplit(1) + "' AND INSTANCE='" + ChSplit(2) + "' AND TIME BETWEEN " + StartTime.ToString() + " AND " + FinishTime.ToString()
            Dim getRows As DataRow() = GetLogs("TIME,DATA", CondStr, GetCatNum(ChSplit(0)) - 1, "TIME DESC")
            Dim GetColl As New List(Of Object)
            If IsNothing(getRows) Then Return Nothing

            ' Can't compress dataset as time will be unique for each record
            For Each rowNum In getRows
                GetColl.Add(rowNum(0).ToString() + "," + rowNum(1).ToString())
            Next
            'For rowNum = 0 To getRows.Length - 1
            'If rowNum <> 0 And rowNum <> (getRows.Length - 1) Then              'only add if the previous or next record is different to compress dataset
            'If getRows(rowNum)(1).ToString() <> getRows(rowNum - 1)(1).ToString() Or getRows(rowNum)(1).ToString() <> getRows(rowNum + 1)(1).ToString() Then GetColl.Add(getRows(rowNum)(0).ToString() + "," + getRows(rowNum)(1).ToString())
            'Else
            'GetColl.Add(getRows(rowNum)(0).ToString() + "," + getRows(rowNum)(1).ToString())
            'End If
            'Next
            'Dim record, oldY As String
            'For Each dr As DataRow In getRows
            '    record = dr(0).ToString + "," + dr(1).ToString
            '    If oldY <> dr(1).ToString Then GetColl.Add(record)
            '    oldY = dr(1).ToString
            '    'Dim DictRow As Dictionary(Of String, Object) = dr.Table.Columns.Cast(Of DataColumn)().ToDictionary(Function(col) col.ColumnName, Function(col) dr.Field(Of Object)(col.ColumnName))
            '    'GetColl.Add(DictRow)
            'Next
            Return GetColl
        End Function

        'Save ini settings from the client, in format XXXX_YYYYY=VAL where XXXX is the section name, YYYYY is the key, VAL is the value, concatinated with '|'
        Private Function SetIniSettings(IniStr As String) As Boolean
            Try
                Dim Settings() As String = IniStr.Split("|"c)           ' Build a string array
                Dim SectionVal(3) As String
                For Each Setting In Settings
                    SectionVal(0) = Setting.Substring(0, Setting.IndexOf("="c))     ' Extract the key and the data either side of the = sign
                    SectionVal(1) = Setting.Substring(Setting.IndexOf("="c) + 1)
                    SectionVal(2) = SectionVal(0).Substring(SectionVal(0).IndexOf("_"c) + 1)  ' Extract the section name before the '_'
                    Ini.Set(SectionVal(0).Substring(0, SectionVal(0).IndexOf("_"c)), SectionVal(2), SectionVal(1))
                Next
                Ini.WriteIniSettings()
            Catch ex As Exception
                MsgBox(ex.ToString)
            End Try
        End Function

        Private Function GetIniSettings(IniString As String) As Array
            Dim SettingArray As New ArrayList
            Dim Settings() As String = IniString.Split(","c)
            Dim SectionVal(1) As String
            For Each setting In Settings
                SectionVal = setting.Substring(1).Split("]"c)
                SettingArray.Add(SectionVal(0) + "_" + SectionVal(1) + "=" + Ini.Get(SectionVal(0), SectionVal(1), ""))
            Next
            Return SettingArray.ToArray
        End Function

        ' Process system actions that happen on the change of a year
        Private Sub ProcessYearChange()

        End Sub

        ' Process system actions that happen on the change of a month
        Private Sub ProcessMonthChange()
        End Sub

        ' Process system actions that happen on the change of a day
        Private Sub ProcessWeekChange()

        End Sub

        ' Process system actions that happen on the change of a day
        Private Sub ProcessDayChange()
            Select Case ArchiveFreq.ToUpper
                Case Is = "MONTHLY"
                    If Date.Now > LogCreated.AddMonths(1) Then ArchiveLog()
                Case Is = "THREE MONTHLY"
                    If Date.Now > LogCreated.AddMonths(3) Then ArchiveLog()
                Case Is = "SIX MONTHLY"
                    If Date.Now > LogCreated.AddMonths(6) Then ArchiveLog()
                Case Is = "YEARLY"
                    If Date.Now > LogCreated.AddMonths(12) Then ArchiveLog()
            End Select

        End Sub

        ' Process system actions that happen on the change of a hour
        Private Sub ProcessHourChange()
            CalcSunRiseSet()                    ' Adjust the sunrise / sunset times (done hourly as hour to switch to/from daylight savings will vary by location)
        End Sub

        ' Process system actions that happen on the change of a minute
        Private Sub ProcessMinuteChange(myMessage As Structures.HAMessageStruc)
            SetTimeOfDay()                                                              ' Set the flags to indicate what time of day it is (varies based on sunrise, sunset times)
            Automation.AutomationEvents(myMessage)                                    ' Each minute check if any of the timer triggers are activated
        End Sub

        ' Process system actions that happen on the change of a second
        Private Sub ProcessSecondChange()

        End Sub

        ' Process actions that happen on the change of a tick
        Private Sub ProcessTickChange()
        End Sub

        ' Submit a message to the event message queue. Requires the message structure to be prepopulated
        Private Function SubmitMessage(HAMessage As Structures.HAMessageStruc) As String
            'HAMessage.Time = DateTime.Now ' Automatic timestamp, For time accurate timestamps use the text in the data field
            'MsgBox("Handle " + HAMessage.Data)
            Try
                If ServiceState <> HAConst.ServiceState.STOPPED And MessageBQ.Count < 1000 Then
                    MessageBQ.Add(HAMessage) ' Don't add messages to the message queue if service is stopped, and if it is paused for too long start dropping messages
                End If
                Return "OK"
            Catch ex As Exception
                HandleSysErrors(True, "ERROR", "Can't submit message to message queue. Exiting...", ex)   ' XXXXX BAD, NEED TO UNCOUPLE
                Return ex.Message
            End Try
        End Function

        ' If there are system or application errors caused by messages, events or plugins, log them into the system log
        Public Sub HandleAppErrors(ErrorLevel As Byte, Category As String, ClassName As String, Instance As String, Scope As String, Data As String, Optional ex As Exception = Nothing, Optional myMessage As Structures.HAMessageStruc = Nothing)
            Dim ErrorData As String
            If Not IsNothing(ex) Then ErrorData = Data + vbCrLf + "Error:" + vbCrLf + ex.ToString Else ErrorData = Data ' Format the error data
            If Not IsNothing(myMessage) Then
                If myMessage.Func = HAConst.MessFunc.ACTION Then
                    HomeNet.SendClient(myMessage.Instance, HAConst.MessFunc.ERROR, myMessage, "Error handling client request. Error: " + ex.ToString, HAConst.MessLog.MAJOR)
                End If
            End If
            WriteConsole(True, "Processing error occurred: " + ErrorData)
            CreateMessage(ClassName, HAConst.MessFunc.ERROR, ErrorLevel, Instance, Scope, ErrorData, Category)                                    ' Pass the error to the create message routine
        End Sub

        ' Helper wrapper to build the message structure and submit (default is SYSTEM category, default network is me)
        Public Function CreateMessage(ClassName As String, Func As Byte, Level As Byte, Instance As String, Scope As String, Data As String, Optional CatNum As String = "SYSTEM", Optional NetNum As Byte = 255) As Structures.HAMessageStruc
            Try
                Dim NewMsg As Structures.HAMessageStruc
                Dim NetMsg As Byte
                If NetNum = 255 Then NetMsg = GlobalVars.myNetNum Else NetMsg = NetNum ' Default is the current network

                With NewMsg
                    .GUID = System.Guid.NewGuid
                    .Time = Date.UtcNow                                 ' Specify the current time of msg in UTC
                    .Network = NetMsg
                    .Category = CatNum                              ' Optional parameter (used for sending messages back from the result of actions) defaults to system (0)
                    .ClassName = ClassName
                    .Func = Func
                    .Level = Level
                    .Instance = Instance
                    .Scope = Scope
                    .Data = Data
                End With

                If SubmitMessage(NewMsg) = "OK" Then
                    If ClassName <> "TIME" Or Instance <> "TICK" Then
                        SetStoreState(NewMsg)                                           ' Save message in state store
                        ExecuteDB(NewMsg)                                               ' Save to message log
                    End If
                    Return NewMsg
                End If
            Catch ex As Exception                                                                                                       ' If I can't create messages then the system is terminal
                HandleSysErrors(True, "ERROR", "Cannot create system messages. System is unstable...", ex)
            End Try
            Return Nothing
        End Function

        ' Helper routine that saves or updates the data in the state store. Key format <Network>.<Category>.<ClassName>.<Instance>.<Scope>. Network and Categories are strings (case insensitive)
        Public Sub SetStoreState(SetState As Structures.HAMessageStruc)
            Dim myKey As Structures.StateStoreKey
            myKey.Network = GetNetName(SetState.Network).ToUpper
            myKey.Category = SetState.Category.ToUpper
            myKey.ClassName = SetState.ClassName.ToUpper
            myKey.Instance = SetState.Instance.ToUpper
            myKey.Scope = SetState.Scope.ToUpper
            HAStateStore(myKey) = SetState.Data
        End Sub

        ' Helper routine that gets the data in the state store. Network and Catefories are strings (case insensitive)
        Public Function GetStoreState(GetState As Structures.HAMessageStruc) As String
            'Dim KeyStr As String = GetNetName(GetState.Network).ToUpper + "." + GetCatName(GetState.Category).ToUpper + "." + GetState.ClassName.ToUpper + "." + GetState.Instance.ToUpper + "." + GetState.Scope.ToUpper
            Dim myKey As Structures.StateStoreKey
            myKey.Network = GetNetName(GetState.Network).ToUpper
            myKey.Category = GetState.Category.ToUpper
            myKey.ClassName = GetState.ClassName.ToUpper
            myKey.Instance = GetState.Instance.ToUpper
            myKey.Scope = GetState.Scope.ToUpper
            If HAStateStore.ContainsKey(myKey) Then
                Return HAStateStore(myKey)
            Else
                Return Nothing                              ' Key can't be found
            End If
        End Function

        ' Helper routine that searches the state store and returns a list of items that match. Returns multiple matches if using a partial search (leave field blank for wildcards)
        Public Function SearchStoreStates(SearchState As Structures.HAMessageStruc) As Dictionary(Of Structures.StateStoreKey, String)
            If SearchState.Network = 0 And SearchState.Category = "ALL" And SearchState.ClassName = "" And SearchState.Instance = "" And SearchState.Scope = "" Then Return Nothing ' Nothing set so exit false
            Dim FoundDict As Dictionary(Of Structures.StateStoreKey, String) = New Dictionary(Of Structures.StateStoreKey, String)
            Dim myKey As Structures.StateStoreKey
            myKey.Network = GetNetName(SearchState.Network).ToUpper
            myKey.Category = SearchState.Category.ToUpper
            myKey.ClassName = SearchState.ClassName.ToUpper
            myKey.Instance = SearchState.Instance.ToUpper
            myKey.Scope = SearchState.Scope.ToUpper
            For Each Key As Structures.StateStoreKey In HAStateStore.Keys
                If myKey.Network <> "ALL" Then If Key.Network <> myKey.Network Then Continue For
                If myKey.Category <> "ALL" Then If Key.Category <> myKey.Category Then Continue For
                If myKey.ClassName <> "" Then If Key.ClassName <> myKey.ClassName Then Continue For
                If myKey.Instance <> "" Then If Key.Instance <> myKey.Instance Then Continue For
                If myKey.Scope <> "" Then If Key.Scope <> myKey.Scope Then Continue For
                FoundDict.Add(Key, HAStateStore(Key))
            Next
            Return FoundDict
        End Function

#End Region
        '##############################################################################################################################################################################
#Region "Plugin Management"

        ' Plugin calls this function to retrieve the Network number for all messages - Needed for the plugins interface
        Public Function GetNetwork(NetName As String) As Byte Implements Plugins.IHAServer.GetNetwork
            Return GetNetNum(NetName)
        End Function

        ' Function to receive events from a plugin. Format is in JSON using the HAMessage structure
        Public Function PluginEvent(RecvJsonStr As String) As String Implements Plugins.IHAServer.PluginEvent
            Dim ResultStr As String = "OK"
            If PluginsStarted Then
                Try
                    Dim HAMessage As Structures.HAMessageStruc = fastJSON.JSON.ToObject(Of Structures.HAMessageStruc)(RecvJsonStr)
                    Dim PlugMsg As Structures.HAMessageStruc = HS.CreateMessage(HAMessage.ClassName, HAMessage.Func, HAMessage.Level, HAMessage.Instance, HAMessage.Scope, HAMessage.Data, HAMessage.Category, HAMessage.Network)      ' Pass onto Msgqueue for processing
                    SaveLastMsg(PlugMsg)     ' Save in plugin array for checking when sending back to plugin to avoid message echo
                    'SubmitMessage(JSON.Instance.ToObject(Of Structures.HAMessageStruc)(RecvJsonStr))                ' Place the event on the message bus for processing
                Catch ex As Exception
                    ResultStr = ex.ToString
                End Try
            Else
                IniMsgs.Add(RecvJsonStr)
            End If
            Return ResultStr
        End Function

        Public Function LoadPlugins(path As String) As String
            Dim Result As String = ""
            'TODO: Use of the "\" as the file path delimiter will not work in linux, need a more generalised method
            ' TODO: Move some of the templated code in the plugin file to this section, like PluginMgr does
            Try
                Dim SystemPlugin As New Structures.PlugStruc
                SystemPlugin.Category = "SYSTEM"
                SystemPlugin.ClassName = "TIME"
                SystemPlugin.Desc = "Sunrise Sunset"
                SystemPlugin.Type = "SYSTEM"
                Dim SunsetCh As New Structures.ChannelStruc
                SunsetCh.Desc = "Sunset Time"
                SunsetCh.Name = "Sunset"
                SunsetCh.Units = "TIME"
                Dim SystemCh As New List(Of Structures.ChannelStruc)
                SystemCh.Add(SunsetCh)
                Dim SunriseCh As New Structures.ChannelStruc
                SunriseCh.Desc = "Sunrise Time"
                SunriseCh.Name = "Sunrise"
                SunriseCh.Units = "TIME"
                SystemCh.Add(SunriseCh)
                SystemPlugin.Channels = SystemCh
                addPlugin(SystemPlugin, True)

                For Each CatName As Structures.CatStruc In GlobalVars.CategoryColl
                    If Directory.Exists(path + "\" + CStr(CatName.Cat)) Then
                        Dim PluginFiles As String() = Directory.GetFiles(path + "\" + CStr(CatName.Cat), "*.DLL")
                        For Each pluginFile As String In PluginFiles                    ' Check & load all valid plugins in the plugin directory
                            Dim Ass As Assembly
                            Try
                                Ass = Assembly.LoadFrom(pluginFile)
                            Catch ex As Exception
                                Continue For                      ' skip any DLLs that don't have valid assemblies
                            End Try                                 ' NOTE: If copying DLLs or downloading and you get an "Operation not supported" load error here, you have to unblock the file properties
                            For Each objType In Ass.GetTypes
                                If objType.IsPublic = True Then     'Only look at public types
                                    If Not ((objType.Attributes And TypeAttributes.Abstract) = TypeAttributes.Abstract) Then    'Ignore abstract classes
                                        'If GetType(Plugins.IHAPlugin).IsAssignableFrom(objType) Then
                                        If Not (objType.GetInterface("IHAPlugin", True) Is Nothing) Then     'See if this type implements our interface
                                            Dim DomSetup As New AppDomainSetup
                                            With DomSetup
                                                .ApplicationBase = AppDomain.CurrentDomain.BaseDirectory     ' Where the main application assembly is
                                                .PrivateBinPath = FileSystem.CurDir + "\plugins\"          ' Where plugins can be found
                                                .ApplicationName = objType.Name
                                                .LoaderOptimization = LoaderOptimization.SingleDomain           ' ?? What does this do??
                                                .DisallowBindingRedirects = False
                                                .DisallowCodeDownload = True
                                            End With

                                            'Dim hostEvidence As Object() = {New Policy.Zone(SecurityZone.MyComputer)}
                                            'Dim SecEvidence As New Policy.Evidence(hostEvidence, Nothing)

                                            ' TODO: Add 'evidence' to increase security
                                            'Dim PluginDomain As AppDomain = AppDomain.CreateDomain(objType.Name, SecEvidence, DomSetup)   ' Create a new application domain for plugins that are isolated from the main application
                                            Dim PluginDomain As AppDomain = AppDomain.CreateDomain(objType.Name, Nothing, DomSetup)   ' Create a new application domain for plugins that are isolated from the main application
                                            Try

                                                myPlugin = DirectCast(PluginDomain.CreateInstanceFromAndUnwrap(pluginFile, objType.FullName, True, Nothing, Nothing, Nothing, Nothing, Nothing), Plugins.IHAPlugin)
                                                Dim PluginInfo As Structures.PlugStruc = myPlugin.StartPlugin(Me, "")         ' Start plugin (no parameters)
                                                If PluginInfo.Status = "DISABLED" Then
                                                    WriteConsole(True, "Skipping Plugin " + PluginInfo.ClassName + " (" + PluginInfo.Desc + ") as disabled")
                                                Else
                                                    If PluginInfo.Status = "OK" Then
                                                        PluginInfo.AssRef = myPlugin
                                                        PluginInfo.Type = "DOTNET"
                                                        addPlugin(PluginInfo, True)
                                                        ' TODO: Unload from main domain if its needed - may not depending on isolation
                                                        ' Check that I'm not loaded into the default domain
                                                    Else
                                                        WriteConsole(True, "ERROR - Plugin " + PluginInfo.ClassName + " (" + PluginInfo.Desc + ") aborted with error: " + PluginInfo.Status)
                                                    End If
                                                End If
                                                Result = PluginInfo.Status
                                            Catch ex As Exception
                                                WriteConsole(True, "ERROR - Could not load plugin " + objType.FullName + ", error: " + ex.ToString)
                                            End Try
                                        End If
                                    End If
                                End If
                            Next
                        Next
                    End If
                Next
                PluginsStarted = True
                For Each IniMsg In IniMsgs          ' If there are messages saved because they were sent before the plugin completely initialised then send them now
                    PluginEvent(IniMsg)
                Next

            Catch ex As Exception
                MsgBox(ex.ToString)     ' DEBUG
                Result = "System Error - " + ex.ToString                ' XXXXXXXXXXXXXXXXXXXXX
            End Try
            Return Result
        End Function

        ' Save the last message received in the plugin structure to avoid echoing the same message back to the plugin
        Public Function SaveLastMsg(myMessage As Structures.HAMessageStruc) As Boolean
            Dim plugin As New Structures.PlugStruc
            Dim ChName As String = myMessage.Category + "/" + myMessage.ClassName.ToUpper()           ' messages are addressed to channels in form CAT/CLASS/INSTANCE
            If Plugins.TryGetValue(ChName, plugin) Then
                For Each channel In plugin.Channels
                    If channel.Name = myMessage.Instance Then
                        plugin.lastMsg = myMessage
                        Plugins(ChName) = plugin
                        Return True
                    End If
                Next
            End If
            Return False        ' Didn't find a channel record
        End Function

        ' Dynamically add/del channel (modify if existing)
        Public Function modChannel(func As String, cat As String, className As String, modCh As Structures.ChannelStruc) As String
            Dim modPlug As New Structures.PlugStruc
            Plugins.TryGetValue(cat + "/" + className.ToUpper(), modPlug)
            Dim PlugChs As List(Of Structures.ChannelStruc) = modPlug.Channels
            Dim newChs As New List(Of Structures.ChannelStruc)
            Select Case func.ToUpper
                Case Is = "ADD"
                    Dim modified As Boolean = False
                    If Not IsNothing(PlugChs) Then
                        For Each ch As Structures.ChannelStruc In PlugChs
                            If ch.Name.ToUpper = modCh.Name.ToUpper Then
                                newChs.Add(modCh)                   ' Swap out channel if it exists
                                modified = True
                            Else
                                newChs.Add(ch)
                            End If
                        Next
                    End If
                    If modified = False Then newChs.Add(modCh) ' Add the channel if it doesn't already exist
                Case Is = "DEL"
                    For Each ch As Structures.ChannelStruc In PlugChs
                        If ch.Name.ToUpper <> modCh.Name.ToUpper Then newChs.Add(ch)
                    Next
            End Select
            modPlug.Channels = newChs
            Plugins(cat + "/" + className.ToUpper()) = modPlug
            WriteConsole(False, "Channel " + func + " '" + modCh.Name + "' in " + cat + "/" + className)
            Return "OK"
        End Function

        ' Add a plugin, updating the entire plugin if overwrite specified or plugin is new, else only add new channels
        Public Function addPlugin(NewPlugin As Structures.PlugStruc, overwrite As Boolean) As String
            If Not overwrite And Plugins.ContainsKey(NewPlugin.Category + "/" + NewPlugin.ClassName.ToUpper()) Then Return "OK"
            For Each ch In NewPlugin.Channels
                modChannel("ADD", NewPlugin.Category, NewPlugin.ClassName, ch)
            Next
            Dim modPlug As New Structures.PlugStruc
            Plugins.TryGetValue(NewPlugin.Category + "/" + NewPlugin.ClassName.ToUpper(), modPlug)
            If IsNothing(modPlug.Channels) Then modPlug.Channels = New List(Of Structures.ChannelStruc)
            NewPlugin.Channels = modPlug.Channels                   ' Add any existing channels
            Plugins(NewPlugin.Category + "/" + NewPlugin.ClassName.ToUpper()) = NewPlugin
            WriteConsole(True, "Plugin " + NewPlugin.Category + "/" + NewPlugin.ClassName + " (" + NewPlugin.Desc + ") loaded.")
            Return "OK"
        End Function

        ' Function to send events to a plugin. Format is in JSON using the HAMessage structure
        Public Function SendtoPlugin(myMessage As Structures.HAMessageStruc) As String
            Dim ResultStr As String = "OK"
            Try
                Dim checkPlug As New Structures.PlugStruc
                If Plugins.ContainsKey(myMessage.Category + "/" + myMessage.ClassName) Then
                    If Plugins(myMessage.Category + "/" + myMessage.ClassName).lastMsg.GUID = myMessage.GUID Then Return "Rejected echo message"
                End If
                Dim MessageStr As String = fastJSON.JSON.ToJSON(myMessage)
                If Plugins.TryGetValue(myMessage.Category.ToUpper + "/" + myMessage.ClassName.ToUpper, checkPlug) Then      ' Is message for a plugin? Not all messages are for plugins
                    If checkPlug.Type = "DOTNET" Then
                        ResultStr = Plugins(myMessage.Category.ToUpper + "/" + myMessage.ClassName).AssRef.HostEvent(MessageStr) ' Send the message back to the DotNet plugin
                        WriteConsole(False, "Msg sent to dotnet plugin: " + myMessage.Instance + " Data: " + myMessage.Data)
                    End If
                    If checkPlug.Type = "NODEJS" Then
                        ResultStr = HomeNet.SendPlugin(MessageStr) ' Send the message back to the NODEJS plugin
                        WriteConsole(False, "Msg sent to nodejs plugin: " + myMessage.Instance + " Data: " + myMessage.Data)
                    End If
                End If
            Catch ex As Exception
                ResultStr = ex.ToString         ' XXXXXXXXXXXXXXXXXXXXXXXXXXX
            End Try
            If ResultStr <> "OK" Then WriteConsole(True, "Error from plugin processing message " + myMessage.ClassName + ", " + myMessage.Instance + ", " + myMessage.Scope + ", " + myMessage.Data + ". Error " + ResultStr)
            Return ResultStr
        End Function

#End Region
        '##############################################################################################################################################################################
#Region "Misc"

        ' Wrapper for Get data from automation DB
        Public Function GetAutoInfo(Table As String, RecordName As String) As DataRow()
            Select Case Table.ToUpper
                Case Is = "EVENTS"
                    Return Automation.GetEventsInfo(RecordName)
                Case Is = "TRIGGERS"
                    Return Automation.GetTriggersInfo(RecordName)
                Case Is = "ACTIONS"
                    Return Automation.GetActionsInfo(RecordName)
                Case Is = "EVENTACTIONS"
                    Return Automation.GetEventActionsInfo(RecordName)
                Case Is = "EVENTTRIGGERS"
                    Return Automation.GetEventTriggersInfo(RecordName)
            End Select
            Return Nothing
        End Function

        ' Wrappers to keep all routines called via HAServices
        Public Function AddNewEvent(EventName As String, EventDescription As String, Enabled As Boolean, OneShot As Boolean, StartDate As DateTime, StopDate As DateTime, TrigNames() As String, ActionNames() As String) As String
            Return Automation.AddNewEvent(EventName, EventDescription, Enabled, OneShot, StartDate, StopDate, TrigNames, ActionNames)
        End Function
        Public Function UpdateEvent(EventName As String, EventDescription As String, Enabled As Boolean, OneShot As Boolean, StartDate As DateTime, StopDate As DateTime, TrigNames() As String, ActionNames() As String) As String
            Return Automation.UpdateEvent(EventName, EventDescription, Enabled, OneShot, StartDate, StopDate, TrigNames, ActionNames)
        End Function
        Public Function DeleteEvent(EventName As String) As String
            Return Automation.DeleteEvent(EventName)
        End Function
        Public Function AddNewTrigger(TriggerName As String, TriggerDesc As String, Script As String, ScriptParam As String, ScriptCond As Integer, ScriptData As String, ChgCond As Integer,
                              StateCond As Integer, TrigDateFrom As DateTime, TrigTimeFrom As DateTime, TrigDateTo As DateTime, TrigTimeTo As DateTime, Sunrise As Boolean, Sunset As Boolean, Mon As Boolean, Tue As Boolean, Wed As Boolean,
                              Thu As Boolean, Fri As Boolean, Sat As Boolean, Sun As Boolean, DayTime As Boolean, NightTime As Boolean, Fortnightly As Boolean, Monthly As Boolean, Yearly As Boolean, Active As Boolean, Inactive As Boolean, TimeofDay As DateTime, TrigChgMessage As Structures.HAMessageStruc, TrigStateMessage As Structures.HAMessageStruc) As String
            Return Automation.AddNewTrigger(TriggerName, TriggerDesc, Script, ScriptParam, ScriptCond, ScriptData, ChgCond, StateCond, TrigDateFrom, TrigTimeFrom, TrigDateTo, TrigTimeTo, Sunrise, Sunset, Mon, Tue, Wed, Thu, Fri, Sat, Sun, DayTime, NightTime, Fortnightly, Monthly, Yearly, Active, Inactive, TimeofDay, TrigChgMessage, TrigStateMessage)
        End Function
        Public Function UpdateTrigger(TriggerName As String, TriggerDesc As String, Script As String, ScriptParam As String, ScriptCond As Integer, ScriptData As String, ChgCond As Integer,
                              StateCond As Integer, TrigDateFrom As DateTime, TrigTimeFrom As DateTime, TrigDateTo As DateTime, TrigTimeTo As DateTime, Sunrise As Boolean, Sunset As Boolean, Mon As Boolean, Tue As Boolean, Wed As Boolean,
                              Thu As Boolean, Fri As Boolean, Sat As Boolean, Sun As Boolean, DayTime As Boolean, NightTime As Boolean, Fortnightly As Boolean, Monthly As Boolean, Yearly As Boolean, Active As Boolean, Inactive As Boolean, TimeofDay As DateTime, TrigChgMessage As Structures.HAMessageStruc, TrigStateMessage As Structures.HAMessageStruc) As String
            Return Automation.UpdateTrigger(TriggerName, TriggerDesc, Script, ScriptParam, ScriptCond, ScriptData, ChgCond, StateCond, TrigDateFrom, TrigTimeFrom, TrigDateTo, TrigTimeTo, Sunrise, Sunset, Mon, Tue, Wed, Thu, Fri, Sat, Sun, DayTime, NightTime, Fortnightly, Monthly, Yearly, Active, Inactive, TimeofDay, TrigChgMessage, TrigStateMessage)
        End Function
        Public Function DeleteTrigger(TriggerName As String) As String
            Return Automation.DeleteTrigger(TriggerName)
        End Function
        Public Function AddNewAction(ActionName As String, ActionDescription As String, Delay As Integer, Random As Boolean, Script As String, ScriptParam As String, Optional HAMessage As Structures.HAMessageStruc = Nothing) As String
            Return Automation.AddNewAction(ActionName, ActionDescription, Delay, Random, Script, ScriptParam, HAMessage)
        End Function
        Public Function UpdateAction(ActionName As String, ActionDescription As String, Delay As Integer, Random As Boolean, Script As String, ScriptParam As String, Optional HAMessage As Structures.HAMessageStruc = Nothing) As String
            Return Automation.UpdateAction(ActionName, ActionDescription, Delay, Random, Script, ScriptParam, HAMessage)
        End Function
        Public Function DeleteAction(ActionName As String) As String
            Return Automation.DeleteAction(ActionName)
        End Function

        ' Run the Action outside of event processing
        Public Sub RunAction(ActionName As String)
            Dim ActionRow() As DataRow = Automation.GetActionsInfo(ActionName)
            If ActionRow.Count > 0 Then Automation.ProcessAction(ActionRow(0))
        End Sub

        ' Set the flags for sunrise, sunset, night, day according to the time of day, using offsets for an extended sunrise/set time, and fire events when the state changes
        Public Sub SetTimeOfDay()
            Dim TimeNow As Date = Date.Now
            If TimeNow.Hour > 12 Then                                                                           ' Afternoon or evening
                Select Case DateDiff(DateInterval.Minute, TimeNow, SunSetTime)
                    Case Is = 0
                        CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "SUNSET", "CHG", TimeNow.ToString) ' Raise event for change to Sunset (at the exact sunset time, not the offset)
                    Case Is > SunriseSetOffset                                                       ' Before (sunset - offset) time
                        IsDay = True : IsSunset = False : IsNight = False : IsSunrise = False                   ' Afternoon
                    Case (-1 * SunriseSetOffset) To SunriseSetOffset                      ' After (sunset - offset) time but before (sunset + offset)
                        IsDay = False : IsSunset = True : IsNight = False : IsSunrise = False                   ' Sunset
                    Case Is < (-1 * SunriseSetOffset)                                                ' After (sunset + offset) time
                        If IsSunset = True Then CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "NIGHT", "CHG", TimeNow.ToString) ' Raise event for change to night
                        IsDay = False : IsSunset = False : IsNight = True : IsSunrise = False                   ' Night
                End Select
            Else                                                                                                ' Early morning or morning
                Select Case DateDiff(DateInterval.Minute, Date.Now, SunRiseTime)
                    Case Is = 0
                        CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "SUNRISE", "CHG", TimeNow.ToString) ' Raise event for change to Sunrise (at the exact sunrise time, not the offset)
                        ' Raise event for Sunset
                    Case Is > SunriseSetOffset                                                       ' Before (sunrise - offset) time
                        IsDay = False : IsSunset = False : IsNight = True : IsSunrise = False                   ' Night
                    Case (-1 * SunriseSetOffset) To SunriseSetOffset                      ' After (sunrise - offset) time but before (sunrise + offset)
                        IsDay = False : IsSunset = False : IsNight = False : IsSunrise = True                   ' Sunrise
                    Case Is < (-1 * SunriseSetOffset)                                                ' After (sunrise + offset) time
                        If IsSunrise = True Then CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "DAY", "CHG", TimeNow.ToString) ' Raise event for change to daytime
                        IsDay = True : IsSunset = False : IsNight = False : IsSunrise = False                   ' Day
                End Select
            End If

        End Sub

        ' To get timezone, city and long/latitude automatically during a setup program, use this free service: http://www.geobytes.com/IpLocator.htm?GetLocation&Template=XML.txt, returns XML with a HTTP get
        ' Calculate sunrise and sunset times based on the longitute and latitude settings in system settings. 
        Public Sub CalcSunRiseSet()
            Try
                Dim oldSunrise = SunRiseTime
                Dim oldSunset = SunSetTime

                'MsgBox("Deg " + Console.TBLatitude.Text.Substring(0, Console.TBLatitude.Text.IndexOf(",")) + "  Min " + Console.TBLatitude.Text.Substring(Console.TBLatitude.Text.IndexOf(",") + 1))
                Dim LatDegrees As Integer = CInt(Latitude.Substring(0, Latitude.IndexOf(",")).Trim)
                If LatDegrees < -90 Or LatDegrees > 90 Then MsgBox("Error - Latitude degrees must be between -90 and 90. Please enter as <Degrees>, <Minutes>. For example, -27, 1.", MsgBoxStyle.Exclamation) : Exit Sub
                Dim LatMinute As Integer = CInt(Latitude.Substring(Latitude.IndexOf(",") + 1).Trim)
                If LatMinute < 0 Or LatDegrees > 60 Then MsgBox("Error - Latitude minutes must be between 0 and 60. Please enter as <Degrees>, <Minutes>. For example, -27, 1.", MsgBoxStyle.Exclamation) : Exit Sub
                Dim LongDegrees As Integer = CInt(Longitude.Substring(0, Longitude.IndexOf(",")).Trim)
                If LongDegrees < -180 Or LongDegrees > 180 Then MsgBox("Error - Longitude degrees must be between -180 and 180. Please enter as <Degrees>, <Minutes>. For example, -27, 1.", MsgBoxStyle.Exclamation) : Exit Sub
                Dim LongMinute As Integer = CInt(Longitude.Substring(Longitude.IndexOf(",") + 1).Trim)
                If LongMinute < 0 Or LongMinute > 60 Then MsgBox("Error - Longitude minutes must be between 0 and 60. Please enter as <Degrees>, <Minutes>. For example, -27, 1.", MsgBoxStyle.Exclamation) : Exit Sub

                Dim SunRiseSet As New SunriseAndSunset
                SunRiseSet.CalculateSolarTimes(SunRiseSet.ConvertfromDegMin(Latitude), SunRiseSet.ConvertfromDegMin(Longitude), DateTime.Now, SunRiseTime, SolarNoonTime, SunSetTime)
                If oldSunrise <> SunRiseTime Then CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.MINOR, "SUNRISE", "TIME", SunRiseTime.ToShortTimeString)
                If oldSunset <> SunSetTime Then CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.MINOR, "SUNSET", "TIME", SunSetTime.ToShortTimeString)
            Catch ex As Exception
                '''MsgBox("Error in Latitude / Longitude settings, Sunrise/Sunset times won't be calculated. Please enter as <Degrees>, <Minutes>. For example, -27, 1.", MsgBoxStyle.Exclamation)
            End Try
        End Sub

        ' Get Category Number from category Name
        Public Function GetCatNum(CatName As String) As Byte
            For Lp As Byte = 0 To CByte(GlobalVars.CategoryColl.Count)
                If CType(GlobalVars.CategoryColl(Lp), Structures.CatStruc).Cat.ToUpper = CatName.ToUpper Then Return Lp
            Next
            Return 255
        End Function

        ' Get Category name from category number (zero index)
        Public Function GetCatName(CatNum As Byte) As String
            If CatNum <= GlobalVars.CategoryColl.Count - 1 Then
                Return CType(GlobalVars.CategoryColl(CatNum), Structures.CatStruc).Cat
            Else
                Return ""
            End If
        End Function

        ' Get Network name from network number (zero index)
        Public Function GetNetName(NetNum As Byte) As String
            If NetNum <= GlobalVars.NetworkColl.Count - 1 Then
                Return GlobalVars.NetworkColl(NetNum).ToString
            Else
                Return ""
            End If
        End Function

        ' Get Network Number from category Name
        Public Function GetNetNum(NetName As String) As Byte
            If NetName = "" Then
                Return GlobalVars.myNetNum                                ' Return the current network number
            Else
                For Lp As Byte = 0 To CByte(GlobalVars.NetworkColl.Count)
                    If GlobalVars.NetworkColl(Lp).ToString.ToUpper = NetName.ToUpper Then Return Lp
                Next
            End If
            Return 255                                                      ' Not found
        End Function

        ' THREAD: Internal timer that generates tick message events, runs on its own thread so needs to be thread safe
        Private Sub TimerLoop()
            Dim TimeNow As DateTime
            Dim ElapsedTime As New Stopwatch
            Dim TimetoSleep As Integer
            Try
                EnableTimer = True                                 ' Toggle for the timer, starts enabled, turned on/off with UI
                Do
                    TimeNow = DateTime.Now
                    ElapsedTime.Restart()
                    If EnableTimer And ServiceState = HAConst.ServiceState.RUNNING Then
                        CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "TICK", "CHG", TimeNow.ToString + "." + TimeNow.Millisecond.ToString)              ' tick message
                        If OldTime.Second <> TimeNow.Second Then   ' Process each second
                            CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "SECOND", "CHG", TimeNow.ToString)
                            If OldTime.Minute <> TimeNow.Minute Then   ' Process each minute
                                CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "MINUTE", "CHG", TimeNow.ToString)
                                If OldTime.Hour <> TimeNow.Hour Then       ' Process each hour
                                    CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "HOUR", "CHG", TimeNow.ToString)
                                    If OldTime.Day <> TimeNow.Day Then
                                        CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "DAY", "CHG", TimeNow.ToString)
                                        If OldTime.DayOfWeek = DayOfWeek.Sunday And TimeNow.DayOfWeek = DayOfWeek.Monday Then
                                            CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "WEEK", "CHG", TimeNow.ToString)
                                            If OldTime.Day <> TimeNow.Month Then
                                                CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "MONTH", "CHG", TimeNow.ToString)
                                                If OldTime.Day <> TimeNow.Year Then
                                                    CreateMessage("TIME", HAConst.MessFunc.EVENT, HAConst.MessLog.NONE, "YEAR", "CHG", TimeNow.ToString)
                                                End If
                                            End If
                                        End If
                                    End If
                                End If
                            End If
                        End If
                    End If
                    OldTime = TimeNow
                    TimetoSleep = TimerTick - CInt(ElapsedTime.ElapsedMilliseconds)
                    If TimetoSleep > 0 Then Thread.Sleep(TimetoSleep) ' Ensure each loop takes TimerTick msec regardless of processing time
                Loop
            Catch ex As Exception
                HandleSysErrors(True, "ERROR", "Error with the timer loop. Exiting...", ex)
            End Try
        End Sub

#End Region
        '##############################################################################################################################################################################
#Region "Start/Stop"

        Public Sub StartServer()
            ServiceState = HAConst.ServiceState.RUNNING                  ' Start processing messages on the queue
        End Sub

        ' Pause server stops processing but messages bank up on the message queue (and may run out of memory if left for too long....)
        Public Sub PauseServer()
            'HATimer.Stop()
            ServiceState = HAConst.ServiceState.PAUSE                    ' Stop processing messages on the queue but allow the queue to back up with messages
        End Sub

        Public Function ServerState() As Integer
            Return ServiceState
        End Function

        Public Sub StopServer()

            ' TODO: Send shutdown cmd to node first
            Try
                NodeProc.Kill()                                         ' Stop Node plugins
            Catch ex As Exception
            End Try

            If ServerState() = HAConst.ServiceState.PAUSE Then
                For Each HAMessage As Structures.HAMessageStruc In MessageBQ
                    MessageBQ.Take()                                                ' Remove any remaining messages from the message queue if we were paused and now stopped.
                Next
            End If
            Dim LoopCnt As Integer = 0
            ' Wait until the message queue is cleared
            Do While MessageBQ.Count <> 0
                LoopCnt = LoopCnt + 1
                If LoopCnt = 500 Then Exit Do ' Timed out waiting for messages to process, shutting down anyway.....
                Thread.Sleep(5)
            Loop
            LoopCnt = 0
            Do While DBActionBQ.Count <> 0                  ' Check if there are any more database entries to process
                LoopCnt = LoopCnt + 1
                If LoopCnt = 500 Then Exit Do ' Timed out waiting for messages to process, shutting down anyway.....
                Thread.Sleep(5)
            Loop

            For Each kvp As KeyValuePair(Of String, Structures.PlugStruc) In Plugins
                Dim myPlugin = kvp.Value
                Try
                    myPlugin.AssRef.StopPlugin("")               ' Stop the plugin
                    ' TODO: SEND A MESSAGE TO NODEJS PLUGIN MGR TO SHUTDOWN ALL PLUGINS
                Catch                                       ' Sometimes while shutting down application, remoting errors are raised, ignore
                End Try
                myPlugin.Status = "STOPPED"
                Plugins(kvp.Key) = myPlugin
            Next
            'For Each Plugin As Structures.PlugStruc In Plugins
            '    Try
            '        Plugin.AssRef.StopPlugin("")               ' Stop the plugin
            '        ' TODO: SEND A MESSAGE TO NODEJS PLUGIN MGR TO SHUTDOWN ALL PLUGINS
            '    Catch                                       ' Sometimes while shutting down application, remoting errors are raised, ignore
            '    End Try
            '    Plugin.Status = "STOPPED"
            'Next

            'TODO: Shutdown node.js plugins

            ServiceState = HAConst.ServiceState.STOPPED
        End Sub

        Public Sub Shutdown()
            Try
                For Lp = 0 To 50                            ' Try for 1/2 second to clear the DB logs before shutting down
                    If DBActionBQ.Count = 0 Then Exit For
                    Thread.Sleep(10)
                Next

                For Lp = 0 To GlobalVars.CategoryColl.Count - 2             ' ignore ALL category
                    CType(LogConn.Item(Lp), SQLite.SQLiteConnection).Close()            ' Close all open log files
                Next

            Catch ex As Exception
                HandleSysErrors(True, "WARNING", "Can't close the database. Exiting...", ex)
            End Try
        End Sub

#End Region

        '##############################################################################################################################################################################

        Private Sub scrapstuff()
            Dim SQLSelCmd As SQLite.SQLiteCommand                ' Command for Select SQL (reused with parameters)
            Dim SQLInsCmd As SQLite.SQLiteCommand                ' Command for Select SQL (reused with parameters)
            Dim SQLOtherCmd As SQLite.SQLiteCommand             ' Command for miscellaneous SQL commands
            Dim SQLreader As SQLite.SQLiteDataReader             ' Read from the database

            Dim DBMsg As Structures.HAMessageStruc
            SQLInsCmd.CommandText = "INSERT INTO MessLog (time, func, level, network, category, class, instance, scope, data) VALUES (@TimeVal, @FuncVal, @LevelVal, @NetworkVal, @CategoryVal, @ClassVal, @InstanceVal, @ScopeVal, @DataVal)"
            Dim TimeVal As SQLite.SQLiteParameter = New SQLite.SQLiteParameter("@TimeVal")
            Dim FuncVal As SQLite.SQLiteParameter = New SQLite.SQLiteParameter("@FuncVal")
            Dim LevelVal As SQLite.SQLiteParameter = New SQLite.SQLiteParameter("@LevelVal")
            Dim NetworkVal As SQLite.SQLiteParameter = New SQLite.SQLiteParameter("@NetworkVal")
            Dim CategoryVal As SQLite.SQLiteParameter = New SQLite.SQLiteParameter("@CategoryVal")
            Dim ClassVal As SQLite.SQLiteParameter = New SQLite.SQLiteParameter("@ClassVal")
            Dim InstanceVal As SQLite.SQLiteParameter = New SQLite.SQLiteParameter("@InstanceVal")
            Dim ScopeVal As SQLite.SQLiteParameter = New SQLite.SQLiteParameter("@ScopeVal")
            Dim DataVal As SQLite.SQLiteParameter = New SQLite.SQLiteParameter("@DataVal")
            SQLInsCmd.Parameters.Add(TimeVal)
            SQLInsCmd.Parameters.Add(FuncVal)
            SQLInsCmd.Parameters.Add(LevelVal)
            SQLInsCmd.Parameters.Add(NetworkVal)
            SQLInsCmd.Parameters.Add(CategoryVal)
            SQLInsCmd.Parameters.Add(ClassVal)
            SQLInsCmd.Parameters.Add(InstanceVal)
            SQLInsCmd.Parameters.Add(ScopeVal)
            SQLInsCmd.Parameters.Add(DataVal)
            TimeVal.Value = DBMsg.Time                      ' Use pre-built commmand with parameters saves CPU cycles
            FuncVal.Value = DBMsg.Func
            LevelVal.Value = DBMsg.Level
            NetworkVal.Value = DBMsg.Network
            CategoryVal.Value = DBMsg.Category
            ClassVal.Value = DBMsg.ClassName
            InstanceVal.Value = DBMsg.Instance
            ScopeVal.Value = DBMsg.Scope
            DataVal.Value = DBMsg.Data
        End Sub
    End Class
End Namespace

