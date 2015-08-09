' Library Imports (add references to the the bin\lib directory)
Imports Commons                 ' Common interfaces and structures for HA Server & plugins
Imports fastJSON                ' FastJSON http://www.codeproject.com/KB/IP/fastJSON.aspx

' CLR namespaces

' HAplugin template
' Usage:    Change namespace name to the category of the plugin (eg. Weather)
'           Change the class name to the class name of the plugin (eg. temperature)
'           Apart from the above, only modify code below the ---------- line for plugin specific code

Namespace Weather                                                                           ' Plugin category
    ' Can put these in the interface file

    Public Class Temperature                                                                ' Plugin class name
        Inherits MarshalByRefObject                                                         ' Needed for application domains
        Implements Plugins.IHAPlugin                                                        ' Plugin interfaces with host

        Public DBSTOPW As New Stopwatch 'XXXXXXXXXXXXXXXXXX

        Public ClassName, Instance As String, Category, Network As Byte        ' Message defaults for this plugin
        Public myhost As Plugins.IHAServer = Nothing                               ' Declare the reference to the host for calling host function 
        Dim IniSettings As New Utils.IniFile("plugins\system")                     ' Open and cache the ini file settings

#Region "Framework code - DO NOT MODIFY"
        ' Startup and initialize the plugin. Setup the plugin framework then call any user startup code
        Private Function PlugStart(ByRef IHost As Plugins.IHAServer, StartParamStr As String) As String Implements Plugins.IHAPlugin.StartPlugin
            Try
                myhost = IHost                                                                      ' Reference to host functions via interface
                Dim FullName As String() = DirectCast(Me, Object).GetType.ToString.Split("."c)       ' Fullname is in format FileName.NameSpace.ClassName
                Network = myhost.GetNetwork("")                                                 ' Get the current network number
                Category = myhost.GetCategory(FullName(1))                                      ' Get the category number from the assembly information
                ClassName = FullName(2)
                Dim PluginThread As New Threading.Thread(AddressOf PluginRun)               ' FIXXXX
                PluginThread.IsBackground = True
                PluginThread.Start()                                                                ' Start main plugin code on a different thread
                Return "OK"
            Catch ex As Exception
                Return ex.ToString
            End Try
        End Function

        ' Shutdown the plugin, release any memory etc.
        Private Function PlugStop(StopParamStr As String) As String Implements Plugins.IHAPlugin.StopPlugin
            Return PluginShutdown(StopParamStr)     ' Call plugin specific shutdown code
        End Function

        ' Raise an event on the host from the plugin
        Private Function RaiseHostEvent(SendMsg As Structures.HAMessageStruc) As String
            Dim MessageStr As String = JSON.Instance.ToJSON(SendMsg)
            Return myhost.PluginEvent(MessageStr)                                           ' Call the host method to receive the data from the plugin
        End Function

        ' Receive an event from the host
        Private Function HostEvent(RecvJSONStr As String) As String Implements Plugins.IHAPlugin.HostEvent
            Try
                RecvHostEvent(JSON.Instance.ToObject(Of Structures.HAMessageStruc)(RecvJSONStr))           ' Process the message
            Catch ex As Exception           ' Incorrect JSON
                'TODO: Send to messagelog that we have a JSON error
                Return "ERROR: Incorrect JSON in routine HostEvent (" + RecvJSONStr + ") " + ex.ToString
            End Try
            Return "OK"
        End Function

        ' Helper wrapper to build the message structure and submit
        Public Function CreateMessage(Func As Byte, Level As Byte, Instance As String, Scope As String, Data As String) As Boolean
            Try
                Dim NewMsg As Structures.HAMessageStruc
                With NewMsg
                    .Time = DateTime.Now                                  ' Set in message queue submission, Must have a unique time tick so can't rely on the calling routine to specify the time (use the data column instead if logging)
                    .Network = Network
                    .Category = Category
                    .ClassName = ClassName
                    .Func = Func
                    .Level = Level
                    .Instance = Instance
                    .Scope = Scope
                    .Data = Data
                End With
                RaiseHostEvent(NewMsg)                                      ' Send message to server
                Return True
            Catch ex As Exception
                'Console.HandleSysErrors(True, "ERROR", "Cannot create system messages. Press OK to exit", ex)
                'Application.Exit()
                Return False
            End Try
        End Function

        ' notes: Be sure to not pass any Type/Assembly/etc. instances (besides your MarshalByRefObject type) back to the original appdomain as this will cause the plugin
        ' to be loaded back in the parent domain

#End Region
        '##############################################################################################################################################################################
#Region "User Routines"

        '  User function to handle plugin startup & running. Most work gets done here - initialise then loop as required. Runs on a separate thread
        Public Sub PluginRun(StartParam As Object)
            Dim StartParamStr As String = DirectCast(StartParam, String)
            ' Plugin initialization code goes here
            'CreateMessage(HAConstants.FUNC_EVT, HAConstants.LOG_NORMAL, "Outdoor", "Sensor1", CByte(1 + Rnd() * 44).ToString)
            DBSTOPW.Start()
            CreateMessage(HAConstants.FUNC_ACT, 2, "Outdoor", "DB:GETTEST(0)", "Select data, instance from  messlog where data = 45")
        End Sub

        ' User function to handle Plugin shutdown
        Public Function PluginShutdown(StartParamStr As String) As String
            Dim Result As String = "OK"
            ' Plugin shutdown code goes here

            Return Result
        End Function

        ' User function to handle events received from the host
        Public Function RecvHostEvent(MyMessage As Commons.Structures.HAMessageStruc) As String
            Dim Result As String = "OK"
            ' Plugin user code 

            Select Case MyMessage.Func                                                  ' Work out what to do. In the format of <scope_action>:<function>
                Case Is = HAConstants.FUNC_RSP, HAConstants.FUNC_ACT                    ' Action requests + received data callbacks handled the same way
                    Dim FuncDelimiter As Integer = MyMessage.Scope.IndexOf(":")
                    If FuncDelimiter > 0 And FuncDelimiter < (MyMessage.Scope.Length - 1) Then                               ' Have I specified a returning routine?
                        Dim myFunc As String = MyMessage.Scope.Substring(0, FuncDelimiter)
                        Dim myScope As String = MyMessage.Scope.Substring(FuncDelimiter + 1)
                        Dim myInst As String = myScope.Substring(myScope.IndexOf("(") + 1).TrimEnd(")"c)
                        myScope = myScope.Substring(0, myScope.IndexOf("("))
                        'MsgBox("Func:" + myFunc + " Scope:" + myScope + " Inst:" + myInst)
                        Select Case myFunc
                            Case Is = "DB"                                                                  ' Function scope
                                Select Case myScope     ' Callback routine
                                    Case Is = "TODAYTEMPS"
                                    Case Is = "GETTEST"
                                        'CreateMessage(HAConstants.FUNC_EVT, HAConstants.LOG_NORMAL, "Outdoor", "Sensor1", DBSTOPW.ElapsedMilliseconds.ToString)
                                        'MsgBox(DBSTOPW.ElapsedMilliseconds)
                                        'Call GetTest(MyMessage.Data)
                                End Select
                        End Select
                    End If
                Case Is = HAConstants.FUNC_LOG
                Case Is = HAConstants.FUNC_EVT
                Case Is = HAConstants.FUNC_LOG
            End Select
            Return Result
        End Function

        ' TEst results callback
        Private Sub GetTest(MyJSONData As String)
            Dim JSONDict As New Dictionary(Of String, Object)
            JSONDict = DirectCast(JSON.Instance.Parse(MyJSONData), Dictionary(Of String, Object))
            Dim DataList As List(Of Object) = DirectCast(JSONDict(""), Generic.List(Of Object))          ' Datatable does not store key info (key = tablename). SQLReader does
            Dim RetData(DataList.Count) As Structures.HAMessageStruc
            Dim RowCnt As Integer = 0
            For RecLp = 0 To DataList.Count - 1
                For Lp = 0 To DataList(0).count - 1
                    MsgBox(DataList(RecLp)(Lp))
                Next
            Next
        End Sub

#End Region
    End Class
End Namespace