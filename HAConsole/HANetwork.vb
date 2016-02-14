Imports fastJSON
Imports SuperWebSocket
Imports System.Net
Imports System.Collections.Concurrent
Imports Commons

' For FastJSON deserialiser to work, the same commons file needs to be used by both the client and the server as the assembly info for the type is sent in the JSON string, and it needs to be identical on both sides to work

Namespace HANetwork

    ' Network = ServerName, Category = SYSTEM, ClassName = NETWORK, Instance = ClientName, Scope = <command>, Data = data command

    Class NetServices
        Private WSPort As Integer
        Private WithEvents WSServer As WebSocketServer
        Private WithEvents WSPlugins As WebSocketServer
        'Private sessions As New List(Of WebSocketSession)()

        'Private WSServer As WebSocketServer
        Private MessageTemplate As New Structures.HAMessageStruc

        Public Structure ClientStruc
            Public ClientContext As WebSocketSession                                                         ' Network data including reference to send client messages back
            Public ConnectTime As Date                                                                  ' When the client created the session
            Public LastMsgDate As Date                                                                      ' Last time I received a message
            Public LastMsg As Structures.HAMessageStruc
            Public Subscribed As List(Of String)
        End Structure

        Private SendRespTempl As New Structures.HAMessageStruc

        Dim RespMsg As New Structures.HAMessageStruc

        Public HAClients As New ConcurrentDictionary(Of String, ClientStruc)   ' List of clients by IPaddress and Name
        Public HAPlugins As New ClientStruc                                     ' Plugin WS connection info

        Public Function Init() As Boolean
            Try
                WriteConsole(True, "Starting Network Management...")
                WSPort = CInt(HS.Ini.Get("server", "WSPort", "1066"))                       ' 1066 is not a well known port
                'WSPort = "80"
                With SendRespTempl
                    .Category = "SYSTEM"
                    .ClassName = "NETWORK"
                    .Instance = "SERVER"
                    .Level = HAConst.MessLog.NORMAL
                    .Func = HAConst.MessFunc.RESPONSE
                    .Network = GlobalVars.myNetNum
                End With

                RespMsg = SendRespTempl
                RespMsg.Scope = "CONNECT"
                RespMsg.Data = "OK"

                ' Initialise the server
                WSServer = New WebSocketServer

                Dim SocketRootConfig As New SuperSocket.SocketBase.Config.RootConfig() 
                Dim SocketServerConfig As New SuperSocket.SocketBase.Config.ServerConfig() With {
                    .Name = "HAConsole", _
                    .Ip = "Any", _
                    .Port = WSPort, _
                    .Mode = SuperSocket.SocketBase.SocketMode.Tcp, _
                    .MaxRequestLength = 140000}         'Max message size is 140K
                WSServer.Setup(SocketRootConfig, SocketServerConfig)


                ' TODO: Perhaps look at putting plugin mgr on same WS instance??
                WSPlugins = New WebSocketServer
                Dim PluginRootConfig As New SuperSocket.SocketBase.Config.RootConfig()
                Dim PluginServerConfig As New SuperSocket.SocketBase.Config.ServerConfig() With {
                    .Name = "HAConsolePlug", _
                    .Ip = "Any", _
                    .Port = WSPort + 1, _
                    .Mode = SuperSocket.SocketBase.SocketMode.Tcp, _
                    .MaxRequestLength = 140000}         'Max message size is 140K
                WSPlugins.Setup(PluginRootConfig, PluginServerConfig)

                WriteConsole(True, "WebSockets server initialised on port " + WSPort.ToString)
                If Start() Then Return True Else Return False ' Start the server
            Catch ex As Exception
                WriteConsole(True, "WebSockets server initialisation failed (using port " + WSPort.ToString + "). Exception: " + ex.ToString)
                Return False
            End Try
        End Function

        ' Start the web sockets server
        Public Function Start() As Boolean
            Try
                WSServer.Start()
                WSPlugins.Start()
                WriteConsole(True, "WebSockets server started.")
                Return True
            Catch ex As Exception
                WriteConsole(True, "WebSockets server failed to start. Error: " + ex.ToString)
                Return False
            End Try
        End Function

        ' Stop the WebSockets server
        Public Function [Stop]() As Boolean
            Try
                WSServer.Stop()
                WSPlugins.Stop()
                WriteConsole(True, "WebSockets server on port " + WSPort.ToString + " stopped.")
                Return True
            Catch ex As Exception
                WriteConsole(True, "WebSockets server failed to stop. Error: " + ex.ToString)
                Return False
            End Try
        End Function

        ' THREAD: Called on completion of a message received
        Public Sub WSReceive(session As WebSocketSession, e As String) Handles WSServer.NewMessageReceived
            Dim ClientMsg As String = e
            Dim ClientName As String = ""
            Dim MsgSubmitted As Structures.HAMessageStruc
            If ClientMsg = "" Then
                WriteConsole(False, "Could not understand request from client " + session.RemoteEndPoint.Address.ToString + ". No Data Received.")
            ElseIf ClientMsg.Substring(0, 2) = "{""" Then  ' Simple parsing looking for JSON string
                Dim HAMessage = DirectCast(fastJSON.JSON.Parse(ClientMsg), IDictionary(Of String, Object))
                ClientName = HAMessage("Client").ToString
                'WriteConsole(False, "Command Received from client " + ClientName + ", Message: " + HAMessage("Scope").ToString + " Data: " + HAMessage("Data").ToString)
                If CByte(HAMessage("Func")) = HAConst.MessFunc.ACTION And HAMessage("Category").ToString.ToUpper = "SYSTEM" And HAMessage("ClassName").ToString.ToUpper = "NETWORK" Then                   ' Handle network messages here
                    Select Case HAMessage("Scope").ToString.ToUpper
                        Case Is = "CONNECT"                                                                         ' Command in the form of [CONNECT]ClientName
                            ConnectClient(session, HAMessage("Instance").ToString, HAMessage("Data").ToString)
                        Case Is = "DISCONNECT"
                            DisconnectClient(session, HAMessage("Instance").ToString, HAMessage("Data").ToString)
                        Case Else
                            WriteConsole(False, "Network Command not understood: " + HAMessage("Data").ToString)
                    End Select
                Else
                    MsgSubmitted = HS.CreateMessage(HAMessage("ClassName").ToString, CByte(HAMessage("Func")), CByte(HAMessage("Level")), HAMessage("Instance").ToString, HAMessage("Scope").ToString, HAMessage("Data").ToString, HAMessage("Category").ToString, CByte(HAMessage("Network")))      ' Pass onto Msgqueue for processing
                    Dim ClientRec As New ClientStruc
                    HAClients.TryGetValue(ClientName, ClientRec)        ' Add last message (used to stop message echo back to the client)
                    ClientRec.LastMsg = MsgSubmitted
                    HAClients(ClientName) = ClientRec
                End If
                WriteConsole(False, "RCV GUID: " + MsgSubmitted.GUID.ToString + " classname: " + MsgSubmitted.ClassName + " instance: " + MsgSubmitted.Instance + " scope: " + MsgSubmitted.Scope + " data: " + MsgSubmitted.Data)
            Else
                WriteConsole(False, "Could not understand request from client " + session.RemoteEndPoint.Address.ToString + ". Data Received: " + ClientMsg)
            End If

        End Sub
        ' THREAD: Called on completion of a message received from node.js pluginmgr
        Public Sub PlugReceive(session As SuperWebSocket.WebSocketSession, e As String) Handles WSPlugins.NewMessageReceived
            Dim PluginMsg As String = e
            If PluginMsg.Substring(0, 2) = "{""" Then                                                       ' Simple parsing looking for JSON string
                Dim HAMessage As New Structures.HAMessageStruc
                HAMessage = fastJSON.JSON.ToObject(Of Structures.HAMessageStruc)(PluginMsg)
                If HAMessage.Func = HAConst.MessFunc.ACTION Then                   ' Handle network messages here
                    Select Case HAMessage.Category
                        Case Is = "SYSTEM"
                            Select Case HAMessage.ClassName
                                Case Is = "SETTINGS"
                                    Select Case HAMessage.Instance.ToUpper
                                        Case Is = "GET:CATEGORIES()"
                                            HAMessage.Data = fastJSON.JSON.ToJSON(GlobalVars.CategoryColl.ToArray)
                                            HAMessage.Func = HAConst.MessFunc.RESPONSE
                                            SendPlugin(fastJSON.JSON.ToJSON(HAMessage))
                                            'fastJSON.JSON.Parameters.UseExtensions = False
                                    End Select
                                Case Is = "NETWORK"
                                    Select Case HAMessage.Instance.ToUpper
                                        Case Is = "PLUGINS"                                 ' PluginMgr started, load plugin definitions
                                            Select Case HAMessage.Scope.ToUpper
                                                Case Is = "INIT"
                                                    ' Map JSON data back into the object structure for Plugins
                                                    ''Dim xx = fastJSON.JSON.Parse(HAMessage.Data)
                                                    Dim plugins = fastJSON.JSON.ToObject(Of List(Of Object))(HAMessage.Data)
                                                    '''Dim plugins = DirectCast(xx, List(Of KeyValuePair(Of String, Object)))
                                                    For Each plugin As IDictionary(Of String, Object) In plugins
                                                        '''For Each plugin As KeyValuePair(Of String, Object) In plugins
                                                        Dim JSPlug As New Structures.PlugStruc
                                                        '''Dim plugVal As IDictionary(Of String, Object) = DirectCast(plugin.Value, IDictionary(Of String, Object))
                                                        JSPlug.Category = DirectCast(plugin("category"), String)
                                                        JSPlug.Desc = DirectCast(plugin("desc"), String)
                                                        JSPlug.ClassName = DirectCast(plugin("className"), String)
                                                        JSPlug.Status = DirectCast(plugin("status"), String)
                                                        JSPlug.Type = "NODEJS"
                                                        JSPlug.Channels = New List(Of Structures.ChannelStruc)
                                                        For Each plugChannel As IDictionary(Of String, Object) In DirectCast(plugin("channels"), List(Of Object))
                                                            Dim channel As Structures.ChannelStruc
                                                            channel.Name = DirectCast(plugChannel("name"), String)
                                                            channel.Desc = DirectCast(plugChannel("desc"), String)
                                                            channel.Type = DirectCast(plugChannel("type"), String)
                                                            channel.IO = DirectCast(plugChannel("io"), String)
                                                            channel.Value = ""
                                                            Dim min As Single = 0
                                                            If Single.TryParse(plugChannel("min").ToString, min) Then channel.Min = min
                                                            Dim max As Single = 0
                                                            If Single.TryParse(plugChannel("max").ToString, max) Then channel.Max = max
                                                            channel.Units = DirectCast(plugChannel("units"), String)
                                                            channel.Attribs = New List(Of Structures.ChannelAttribStruc)
                                                            For Each plugChannelAttrib As IDictionary(Of String, Object) In DirectCast(plugChannel("attribs"), List(Of Object))
                                                                Dim ChannelAttrib As Structures.ChannelAttribStruc
                                                                ChannelAttrib.Name = DirectCast(plugChannelAttrib("name"), String)
                                                                ChannelAttrib.Type = DirectCast(plugChannelAttrib("type"), String)
                                                                ChannelAttrib.Value = plugChannelAttrib("value").ToString
                                                                channel.Attribs.Add(ChannelAttrib)
                                                            Next
                                                            JSPlug.Channels.Add(channel)
                                                        Next
                                                        HS.addPlugin(JSPlug, True)
                                                    Next

                                            End Select

                                        Case Else
                                            WriteConsole(False, "System Command not understood: " + HAMessage.Data)
                                    End Select
                            End Select
                        Case Else
                            Select Case HAMessage.Scope
                                Case Is = "ADDCH"                       ' Dynamically add channel from plugin (no channel name used, data has channel ChannelStruc array)
                                    Dim plugChannel As IDictionary(Of String, Object)
                                    plugChannel = DirectCast(fastJSON.JSON.Parse(HAMessage.Data), IDictionary(Of String, Object))
                                    Dim channel As Structures.ChannelStruc
                                    channel.Name = DirectCast(plugChannel("name"), String)
                                    channel.Desc = DirectCast(plugChannel("desc"), String)
                                    channel.Type = DirectCast(plugChannel("type"), String)
                                    channel.IO = DirectCast(plugChannel("io"), String)
                                    channel.Value = ""
                                    Dim min As Single = 0
                                    If Single.TryParse(plugChannel("min").ToString, min) Then channel.Min = min
                                    Dim max As Single = 0
                                    If Single.TryParse(plugChannel("max").ToString, max) Then channel.Max = max
                                    channel.Units = DirectCast(plugChannel("units"), String)
                                    channel.Attribs = New List(Of Structures.ChannelAttribStruc)
                                    For Each plugChannelAttrib As IDictionary(Of String, Object) In DirectCast(plugChannel("attribs"), List(Of Object))
                                        Dim ChannelAttrib As Structures.ChannelAttribStruc
                                        ChannelAttrib.Name = DirectCast(plugChannelAttrib("name"), String)
                                        ChannelAttrib.Type = DirectCast(plugChannelAttrib("type"), String)
                                        ChannelAttrib.Value = plugChannelAttrib("value").ToString
                                        channel.Attribs.Add(ChannelAttrib)
                                    Next
                                    HS.modChannel("ADD", HAMessage.Category, HAMessage.ClassName, channel)
                            End Select
                    End Select
                Else
                    WriteConsole(False, "Plugin Msg Received from: " + HAMessage.ClassName + "(" + HAMessage.Instance + ") Data: " + HAMessage.Data)
                    ' TODO: Consolidate this with the equivalent in the dotnet plugin message with createmessage and submitmessage (do in JSON format to avoid deserializing & re-searalization performance hits)
                    Dim PlugMsg As Structures.HAMessageStruc = HS.CreateMessage(HAMessage.ClassName, HAMessage.Func, HAMessage.Level, HAMessage.Instance, HAMessage.Scope, HAMessage.Data, HAMessage.Category, HAMessage.Network)      ' Pass onto Msgqueue for processing
                    HS.SaveLastMsg(PlugMsg)     ' Save in plugin array for checking when sending back to plugin to avoid message echo
                End If
            Else
                WriteConsole(False, "Could not understand request from client " + session.RemoteEndPoint.Address.ToString + ". Data Received: " + PluginMsg)
            End If

        End Sub

        Public Function SendClient(Client As String, Func As HAConst.MessFunc, myMessage As Structures.HAMessageStruc, DataObj As Object, Optional MsgLevel As HAConst.MessLog = Nothing) As Boolean
            myMessage.GUID = System.Guid.NewGuid                    ' When sending directly back to client (not via the message queue - subscription function), change the message GUID as most routines reuse the received message
            MsgQSend(Client, Func, myMessage, DataObj, MsgLevel)
        End Function

        'Pass the message back to the client after it has been processed, with the data parsed back as the object type
        Public Function MsgQSend(Client As String, Func As HAConst.MessFunc, myMessage As Structures.HAMessageStruc, DataObj As Object, Optional MsgLevel As HAConst.MessLog = Nothing) As Boolean
            fastJSON.JSON.Parameters.UseExtensions = False
            If HAClients.ContainsKey(Client) AndAlso myMessage.GUID <> HAClients(Client).LastMsg.GUID Then  ' Check message GUID, don't send client back the message they just sent

                If MsgLevel <> Nothing Then myMessage.Level = MsgLevel
                myMessage.Func = Func

                Select Case True
                    Case Is = TypeOf DataObj Is String
                        myMessage.Data = DirectCast(DataObj, System.String)
                    Case Is = TypeOf DataObj Is DataTable
                        myMessage.Data = fastJSON.JSON.ToJSON(DirectCast(DataObj, DataTable))
                    Case Is = TypeOf DataObj Is Array
                        myMessage.Data = fastJSON.JSON.ToJSON(DirectCast(DataObj, Array))
                    Case Is = TypeOf DataObj Is Boolean
                        myMessage.Data = fastJSON.JSON.ToJSON(DirectCast(DataObj, System.Boolean))
                    Case Is = TypeOf DataObj Is List(Of Object)
                        myMessage.Data = fastJSON.JSON.ToJSON(DirectCast(DataObj, List(Of Object)))
                    Case Is = TypeOf DataObj Is Dictionary(Of String, String)
                        myMessage.Data = fastJSON.JSON.ToJSON(DirectCast(DataObj, Dictionary(Of String, String)))
                    Case Is = IsNothing(DataObj)
                        myMessage.Data = fastJSON.JSON.ToJSON(Nothing)
                    Case Else
                        Return False                                ' DOn't understand the type
                End Select
                'myMessage.Data = myMessage.Data.Replace("\", "\\")          ' Need to escape backslash character else JSON parsing in Javascript aborts
                Dim EncJSON As String = fastJSON.JSON.ToJSON(myMessage)                                        ' Convert the whole message to JSON
                'WriteConsole(False, "Msg sent to client: " + myMessage.Instance + " Data: " + myMessage.Data)

                WriteConsole(False, "---- Sending to Client: " + Client + " classname: " + myMessage.ClassName + " instance: " + myMessage.Instance + " scope: " + myMessage.Scope + " data: " + myMessage.Data)
                Return SendJSONMsg(Client, EncJSON)
            End If
        End Function

        Public Function SendPlugin(myJSON As String) As String
            Try
                HAPlugins.ClientContext.Send(myJSON)
                Return "OK"
            Catch ex As Exception
                Return ex.Message
            End Try
        End Function

        ' Send raw message in JSON format
        Private Function SendJSONMsg(Client As String, myJSON As String) As Boolean
            Try
                HAClients(Client).ClientContext.Send(myJSON)
                Return True
            Catch
                Return False
            End Try
        End Function

        Private Structure ConnStruc
            Public ClientName As String
            Public ServerName As String
        End Structure

        Private Function ConnectClient(Client As WebSocketSession, ClientName As String, ClientData As String) As Boolean
            If Not IsNothing(ClientName) And ClientName <> "" Then
                Dim RespMsg As New Structures.HAMessageStruc
                RespMsg = SendRespTempl
                If ClientName = "local_machine" Then
                    ClientName = Dns.GetHostEntry(Client.RemoteEndPoint.Address.ToString).HostName      ' XXXX Need to test this across the internet/firewall
                    RespMsg.Scope = "CONNECT"
                Else
                    ' User specified, so do authorisation here....
                    RespMsg.Scope = "AUTHENTICATED"
                End If
                Dim myConn As ConnStruc
                myConn.ClientName = ClientName
                myConn.ServerName = GlobalVars.myNetName
                RespMsg.Data = fastJSON.JSON.ToJSON(myConn)
                If IsNothing(Client.Cookies("clientname")) Then
                    Dim NewClient As New ClientStruc
                    NewClient.ClientContext = Client
                    NewClient.ConnectTime = Date.Now
                    'WriteConsole(True, Dns.GetHostEntry(Client.RemoteEndPoint.Address.ToString).HostName)
                    'NewClient.LastMsg = Date.Now
                    NewClient.Subscribed = New List(Of String)
                    If HAClients.ContainsKey(ClientName) Then ClientName = ClientName + "1" ' TODO Keep adding session numbers
                    HAClients(ClientName) = NewClient                                   ' Create a client session with the name of the client as the key, client context as value (multiple connects OK)
                    Client.Cookies.Add("clientname", ClientName)                                   ' Add client name to the supersocket session object so we can identify it on disconnect
                    WriteConsole(True, "Client '" + ClientName + "' registered from IP address " + Client.RemoteEndPoint.Address.ToString)
                    'Dim jsonText As String = SerialiseHAMsg(RespMsg)
                    'NewClient.ClientContext.Send(jsonText)                      ' send message to client
                Else
                    WriteConsole(True, "Client '" + ClientName + "' trying to reconnect and already logged in")
                End If
                Dim jsonText As String = SerialiseHAMsg(RespMsg)
                HAClients(ClientName).ClientContext.Send(jsonText)                      ' send message to client
            End If
        End Function

        Private Function DisconnectClient(Client As WebSocketSession, ClientName As String, ClientData As String) As Boolean
            WriteConsole(True, "Client '" + ClientName + "' disconnecting, removing subscriptions")
            Dim clearClient As New ClientStruc
            clearClient = HAClients(ClientName)
            clearClient.Subscribed = New List(Of String)
            HAClients(ClientName) = clearClient
        End Function

        Public Shared Function SerialiseHAMsg(cMsg As Structures.HAMessageStruc) As String
            fastJSON.JSON.Parameters.UseExtensions = False
            Return fastJSON.JSON.ToJSON(cMsg)
        End Function

        ' THREAD: Called when a client completes connection. No session is established until the client sends the session details
        Private Sub WSConnected(mySession As WebSocketSession) Handles WSServer.NewSessionConnected
            Try
                WriteConsole(False, "Client connection From IP Address: " + mySession.RemoteEndPoint.Address.ToString)
            Catch ex As Exception
                Console.Write("Error: " + ex.ToString)
            End Try
        End Sub

        ' THREAD: Called when a client completes connection. No session is established until the client sends the session details
        Private Sub WSPluginConn(mySession As WebSocketSession) Handles WSPlugins.NewSessionConnected
            Try
                HAPlugins.ClientContext = mySession
                HAPlugins.ConnectTime = Date.Now
                'HAPlugins.LastMsg = Date.Now
                WriteConsole(False, "Plugin Manager connected From IP Address: " + mySession.RemoteEndPoint.Address.ToString)
            Catch ex As Exception
                Console.Write("Error: " + ex.ToString)
            End Try
        End Sub


        ' THREAD: Called when a client disconnects a session
        Private Sub WSDisconnect(DiscSession As WebSocketSession, e As SuperSocket.SocketBase.CloseReason) Handles WSServer.SessionClosed
            Dim ClientData As New ClientStruc
            If DiscSession.Cookies IsNot Nothing Then
                Dim ClientName As String = DiscSession.Cookies("clientname")                ' Find client name from the session cookie
                If Not IsNothing(ClientName) Then
                    HAClients.TryRemove(ClientName, ClientData) ' Remove from client list
                    WriteConsole(True, "Client '" + ClientName + "' session disconnected from IP address: " + DiscSession.RemoteEndPoint.Address.ToString)
                End If
            Else
                WriteConsole(True, "Disconnect received without an initiated client from IP address: " + DiscSession.RemoteEndPoint.Address.ToString)
            End If
        End Sub

    End Class


End Namespace
