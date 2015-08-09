' Library Imports (add references to the the bin\lib directory)
Imports Commons                 ' Common interfaces and structures for HA Server & plugins
Imports fastJSON                ' FastJSON http://www.codeproject.com/KB/IP/fastJSON.aspx
Imports System.Net.Sockets
Imports System
Imports System.Collections.Generic

' CLR namespaces

' HAplugin template
' Usage:    Change namespace name to the category of the plugin (eg. Weather)
'           Change the class name to the class name of the plugin (eg. temperature)
'           Modify the plugin attributes
'           Apart from the above, only modify code below the ---------- line for plugin specific code

Namespace Utilities                                                                         ' Plugin category
    ' Can put these in the interface file

    Public Class TestPlugin                                                                      ' Plugin class name
        Inherits MarshalByRefObject                                                         ' Needed for application domains
        Implements Plugins.IHAPlugin                                                        ' Plugin interfaces with host

        Private PluginCfg As Structures.PlugStruc
        Private Channels As New List(Of Structures.ChannelStruc)

        Private ClassName, Instance, Category As String, Network As Byte                     ' Message defaults for this plugin
        Private myhost As Plugins.IHAServer = Nothing                                        ' Declare the reference to the host for calling host function 
        Dim IniSettings As Commons.IniFile                                                  ' Open and cache the ini file settings

#Region "Framework code - DO NOT MODIFY"
        ' Startup and initialize the plugin. Setup the plugin framework then call any user startup code
        Private Function PlugStart(ByRef IHost As Plugins.IHAServer, StartParamStr As String) As Structures.PlugStruc Implements Plugins.IHAPlugin.StartPlugin
            Try
                myhost = IHost                                                                      ' Reference to host functions via interface
                Dim FullName As String() = DirectCast(Me, Object).GetType.ToString.Split("."c)      ' Fullname is in format FileName.NameSpace.ClassName
                Network = myhost.GetNetwork("")                                                     ' Get the current network number
                Category = FullName(1)                                          ' Get the category number from the assembly information
                ClassName = FullName(2)                                                             ' Classname is same as the plugin .NET class
                IniSettings = New Commons.IniFile("plugins\" + FullName(1) + "\" + ClassName)       ' Ini Files stored in plugins\<Category>\ClassName.ini

                With PluginCfg
                    .Category = Category.ToUpper
                    .ClassName = ClassName
                    .Desc = IniSettings.Get("PluginCfg", "Desc", ClassName)
                    .Status = "OK"
                    .Channels = Channels
                End With

                If IniSettings.Get("PluginCfg", "Enabled", "").ToLower() <> "true" Then
                    PluginCfg.Status = "DISABLED"
                    Return PluginCfg
                End If

                Dim Name As String
                For ChNum = 0 To 255
                    Name = IniSettings.Get("Channel" + ChNum.ToString, "Name", "")
                    If Name = "" Then Exit For
                    Dim Channel As New Commons.Structures.ChannelStruc
                    Dim Attribs As New List(Of Commons.Structures.ChannelAttribStruc)
                    Channel.Attribs = Attribs
                    With Channel
                        .Name = IniSettings.Get("Channel" + ChNum.ToString, "Name", "")
                        .Desc = IniSettings.Get("Channel" + ChNum.ToString, "Desc", "")
                        .Type = IniSettings.Get("Channel" + ChNum.ToString, "Type", "")
                        .IO = IniSettings.Get("Channel" + ChNum.ToString, "IO", "")
                        .Min = CSng(IniSettings.Get("Channel" + ChNum.ToString, "Min", ""))
                        .Max = CSng(IniSettings.Get("Channel" + ChNum.ToString, "Max", ""))
                        .Units = IniSettings.Get("Channel" + ChNum.ToString, "Units", "")
                        .Value = ""
                    End With
                    Dim Attrib As String
                    For AttribNum = 0 To 255
                        Attrib = IniSettings.Get("Channel" + ChNum.ToString, "Attrib" + AttribNum.ToString, "")
                        If Attrib.IndexOf("(") = -1 Then Exit For ' Attribs in the format of FUNC(VAL) only
                        Dim SplitAttrib() As String = Attrib.Split("(")
                        Channel.Attribs.Add(New Commons.Structures.ChannelAttribStruc With {.Name = "ATTRIB" + AttribNum.ToString, .Type = SplitAttrib(0), .Value = SplitAttrib(1).Trim(")")})
                    Next
                    PluginCfg.Channels.Add(Channel)
                    Channel = Nothing
                    Attribs = Nothing
                Next

                Dim PluginThread As New Threading.Thread(AddressOf PluginRun)
                PluginThread.IsBackground = True
                PluginThread.Start()                                                                ' Start main plugin code on a different thread

                Return PluginCfg
            Catch ex As Exception
                PluginCfg.Status = ex.Message
                Return PluginCfg
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
                'TODO: Send to messagelog that we have a plugin
                Return ex.ToString
            End Try
            Return "OK"
        End Function

        ' Helper wrapper to build the message structure and submit
        Private Function CreateMessage(Func As Byte, Level As Byte, Instance As String, Scope As String, Data As String) As Boolean
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

        Private Function Publish(ChannelName As String, ChannelFunc As String, Data As String) As Boolean
            Return CreateMessage(HAConst.MessFunc.EVENT, HAConst.MessLog.NORMAL, ChannelName, ChannelFunc, Data)
        End Function

        ' notes: Be sure to not pass any Type/Assembly/etc. instances (besides your MarshalByRefObject type) back to the original appdomain as this will cause the plugin
        ' to be loaded back in the parent domain

#End Region
        '##############################################################################################################################################################################
#Region "User Routines"
        '  User function to handle plugin startup & ServiceState.RUNNING. Most work gets done here - initialise then loop as required. Runs on a separate thread
        Public Sub PluginRun(StartParam As Object)
            Dim StartParamStr As String = DirectCast(StartParam, String)
            ' Plugin initialization code goes here

            While True
                'Publish(PluginCfg.Channels(0).Name, "value", CInt(Rnd() * 100).ToString)         ' current value
                Threading.Thread.Sleep(2000)
            End While

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
                Case Is = HAConst.MessFunc.RESPONSE, HAConst.MessFunc.ACTION                     ' Action requests + received data callbacks handled the same way
                Case Is = HAConst.MessFunc.LOG
                Case Is = HAConst.MessFunc.EVENT
                    'MsgBox("From SNMP recvhost " + MyMessage.Instance + " " + MyMessage.Scope + " " + MyMessage.Data)
                    Select Case MyMessage.Scope
                        Case Is = Channels(0).Name
                            If MyMessage.Data.Contains("load") Then
                                Publish(PluginCfg.Channels(0).Name, "load", popTestArray(12312))
                            End If
                    End Select
                Case Is = HAConst.MessFunc.LOG
            End Select
            Return Result
        End Function

        ' Test harness to populate a JSON string with a couple of minutes of data
        Function popTestArray(seed) As String
            Dim currTime As Long = Now.Ticks
            Dim ch As New Dictionary(Of String, Integer)
            Randomize(seed)
            For i = 200 To 0 Step -1
                If Rnd() > 0.2 Then
                    ch.Add((currTime - i * 10000 * 1000).ToString, Int(Rnd() * 100))
                End If
            Next
            fastJSON.JSON.Instance.Parameters.UseExtensions = False
            Return fastJSON.JSON.Instance.ToJSON(ch)

        End Function

#End Region
    End Class
End Namespace