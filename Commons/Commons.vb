' Declare the common interfaces and structures used between the Server host and plugins

Imports fastJSON

Namespace Plugins
    ' Plugin methods the host can call
    Public Interface IHAPlugin
        Function StartPlugin(ByRef IHost As IHAServer, StartParamStr As String) As Structures.PlugStruc         ' Initialise the plugin, pass reference to host so host methods can be called, pass any startup params to initialise code in plugin
        Function StopPlugin(StopParamStr As String) As String                                     ' Shutdown plugin, pass any shutdown params needed
        Function HostEvent(EventStr As String) As String                                          ' Receive events from the Server 
    End Interface

    ' Host server interfaces available for the plugin to call
    Public Interface IHAServer
        Function PluginEvent(RecvParamStr As String) As String                                    ' Send an event from the plugin to the server host
        'Function GetCategory(CatName As String) As Byte                                           ' Get the category number for the category name supplied
        Function GetNetwork(NetName As String) As Byte                                            ' Return the current network number (use "") or the number for a specific network name (use "<networkname>")
    End Interface
End Namespace

Namespace Structures
    ' Main Message format between objects (plugins, network etc)

    <Serializable()>
    Public Structure HAMessageStrucTEST
        Public Property GUID As System.Guid                 ' Unique message identifier
        Public Property Time As DateTime                    ' Timestamp of request (from the originator)
        Public Property Func As Byte                        ' Action or Log or Error message
        Public Property Level As Byte                       ' Log or error level
        Public Property Network As Byte                     ' Network number of source request
        Public Property Category As String                  ' Category of message (eg. Weather)
        Public Property ClassName As String                 ' Generic name of object (eg. Temperature)
        Public Property Instance As String                  ' Specific node instance name (eg. outdoor)
        Public Property Scope As String                     ' item scope (eg. Current temperature) or method name
        Public Property Data As Object                      ' item data (eg. 23 degrees) OR method data
    End Structure

    <Serializable()>
    Public Structure HAMessageStruc
        Public Property GUID As System.Guid                 ' Unique message identifier
        Public Property Time As DateTime                    ' Timestamp of request (from the originator)
        Public Property Func As Byte                        ' Action or Log or Error message
        Public Property Level As Byte                       ' Log or error level
        Public Property Network As Byte                     ' Network number of source request
        Public Property Category As String                  ' Category of message (eg. Weather)
        Public Property ClassName As String                 ' Generic name of object (eg. Temperature)
        Public Property Instance As String                  ' Specific node instance name (eg. outdoor)
        Public Property Scope As String                     ' item scope (eg. Current temperature) or method name
        Public Property Data As String                      ' item data (eg. 23 degrees) OR method data
        Public Property OldData As String                   ' Save the previous value
    End Structure

    <Serializable()>
    Public Structure QueryStruc                             ' Query request against data or state store
        Public Property Func As String
        Public Property StartDateTime As String               ' .NET UTC time format
        Public Property EndDateTime As String
        Public Property TimeFrame As String
        Public Property ValCond As String
        Public Property Cat As String
        Public Property ClassName As String
        Public Property Instance As String
        Public Property Scope As String
        Public Property Data As String
    End Structure

    <Serializable()>
    Public Structure PlugStruc
        Public Category As String
        Public ClassName As String
        Public Desc As String
        Public Type As String
        Public Status As String
        Public Channels As List(Of Structures.ChannelStruc)
        <JsonInclude(False)>
        Public NodeRef As String
        <JsonInclude(False)>
        Public lastMsg As HAMessageStruc                    ' Used to avoid sending back the same message to the plugin
        <JsonInclude(False)>
        Public AssRef As Plugins.IHAPlugin                 ' Pointer to the plugin
    End Structure

    <Serializable()>
    Public Structure ChannelStruc
        Public Name As String
        Public Desc As String
        Public Type As String
        Public IO As String
        Public Min As Single
        Public Max As Single
        Public Units As String
        Public Attribs As List(Of Structures.ChannelAttribStruc)
        Public Value As String
    End Structure

    <Serializable()>
    Public Structure ChannelAttribStruc
        Public Name As String
        Public Type As String
        Public Value As String
    End Structure

    Public Structure StateStoreKey
        Public Network As String
        Public Category As String
        Public ClassName As String
        Public Instance As String
        Public Scope As String
    End Structure

    Public Structure CatStruc
        Public Cat As String
        Public Icon As String
    End Structure

End Namespace

' Global variables for all classes to share
Public Class GlobalVars
    Public Shared myNetName As String = ""                  ' Name of the network for this server
    Public Shared myNetNum As Byte                          ' Number of the network for this server (looked up from the network name list)
    Public Shared CategoryColl As New ArrayList             ' List of all the valid categories for messages
    Public Shared NetworkColl As New ArrayList              ' List of all the valid networks for messages
    'Public Shared ReadytoClose As Boolean = False          ' Flag that all the relevant services are ready to shut down, log files written etc.
End Class

' Global constants for all classes to share
Public Class HAConst

    'Public Shared ReadOnly NULLDATE As DateTime = New DateTime(0)                  
    Public Shared ReadOnly unixEpoc As Long = 621355968000000000
    Public Shared ReadOnly pcTicks As Long = 10000

    ' System category codes (stored in the DB as Bytes)
    Public Enum CatConsts As Byte
        ALL = 0                                             ' Internal category for all messages (used in tests for triggers)
        SYSTEM = 1                                          ' Internal category for system messages
    End Enum

    ' Network category codes (stored in the DB as Bytes)
    Public Enum NetConsts As Byte
        ALL = 0                                             ' Internal category for all networks (used in tests for triggers)
    End Enum

    ' Message Function codes
    Public Enum MessFunc As Byte
        ACTION = 0                                          ' Action or method call
        RESPONSE = 1                                        ' Data response to a method call
        [EVENT] = 2                                         ' Asynchronous event message
        LOG = 3                                             ' Log Message
        [ERROR] = 4                                         ' Error Message
        SQL = 5                                             ' Execute SQL message
    End Enum

    ' Log types (=LEVEL)
    Public Enum MessLog As Byte
        NONE = 0                                            ' Do not write to database log or to the console
        INFO = 1                                            ' Do not write to database log but to console if detailed logs enabled
        MINOR = 2                                           ' Low level function console log
        NORMAL = 3                                          ' Normal function console log
        MAJOR = 4                                           ' Display on main console and all related consoles
    End Enum

    ' Error types (=LEVEL)
    Public Enum MessErr As Byte
        OK_NOLOG = 0                                        ' No error (no writing to log or the console)
        OK_LOG = 1                                          ' No error (no writing to log but to console if detailed logs enabled)
        LOW = 2                                             ' Warning
        MEDIUM = 3                                          ' Something is wrong but can continue functioning (log to function log)
        HIGH = 4                                            ' Something is wrong, likely problems with continuing function (log to function & console log)
    End Enum

    ' Condition tests in triggers
    Public Enum TestCond
        EQUALS = 0                                          ' Equal
        GREATER_THAN = 1
        LESS_THAN = 2
        NOT_EQUAL = 3
        CONTAINS = 4
        CHANGE = 5
    End Enum

    ' Misc types
    Public Enum ServiceState
        STOPPED = 0                                         ' Not processing Messages and they are not saved on the message queue (all new messages lost)
        RUNNING = 1                                         ' Processing Messages as they come onto the queue
        PAUSE = 2                                           ' Not processing Messages but they are backed up on the message queue
    End Enum

    ' Event numbers that define the type of shutdown occurring for the console
    Public Enum ShutTypes
        CTRL_C_EVENT = 0
        CTRL_BREAK_EVENT = 1
        CLOSE_EVENT = 2
        LOGOFF_EVENT = 5
        SHUTDOWN_EVENT = 6
    End Enum

    ' Console error codes (passed to the command line when exiting)
    Public Enum ExitCodes
        OK = 0                                              ' Normal exit code
        ERR = 1                                             ' Fatal error exit code
    End Enum
End Class

Public Class IniFile
    Public Property FileName As String
    Public Property FileLines As String()
    Private FileLock As New Object                          ' Lock to ensure no concurrent reading / writing

    Public Sub New(FileToOpen As String)
        SyncLock FileLock
            FileName = FileToOpen
            FileLines = IO.File.ReadAllLines(FileName + ".ini")         ' cache for reading speed
        End SyncLock
    End Sub

    ' Get a value from an ini file. Will return the first value in the first section found, ignoring lines with ';' comments, returning default if the key can't be found
    Public Function [Get](Section As String, Key As String, DefaultVal As String) As String
        Dim FoundSection As Boolean = False
        Dim KeyName As String
        Dim EquPos As Integer
        Dim FirstChar As Char
        For Lp = 0 To FileLines.Length - 1
            Try
                If FileLines(Lp).Length > 0 Then
                    FirstChar = CChar(FileLines(Lp).Substring(0, 1))
                    If FirstChar <> ";"c Then                                            ' Ignore comments
                        If FoundSection = False And FirstChar <> "["c Then Continue For ' Initially just look for sections
                        If FoundSection = True And FirstChar = "["c Then Exit For ' If we found our section but have not found our key & exited, then key doesnt exist
                        If FileLines(Lp).Trim.ToLower = "["c + Section.ToLower + "]"c Then FoundSection = True ' I found the relevant section
                        EquPos = FileLines(Lp).IndexOf("="c)                             ' Find the position of the '=' to split key & value
                        If EquPos > 1 And FoundSection = True Then                      ' Minimum 1 character as key else we didn't find a valid line
                            KeyName = FileLines(Lp).Substring(0, EquPos - 1).Trim.ToLower
                            If KeyName = Key.ToLower Then
                                If FileLines(Lp).Length = (EquPos + 1) Then Return "" Else Return FileLines(Lp).Substring(EquPos + 1).Trim() ' return empty if no value or extract key value if present
                            End If
                        End If
                    End If
                End If
            Catch                                       ' Ignore lines that throw errors (eg. no characters)
            End Try
        Next
        Return DefaultVal               ' If I get here I have not found the section or key, so return the default value
    End Function

    ' Updates the FileLines cache. 
    Public Function [Set](Section As String, Key As String, WriteVal As String) As Boolean
        SyncLock FileLock
            Dim FoundSection As Boolean = False
            Dim KeyName As String
            Dim EquPos As Integer
            Dim CancelBlank As Integer = 1                                           ' Add one to the string array
            For Lp As Integer = 0 To FileLines.Length - 1
                Try
                    If FileLines(Lp).Length > 1 Then                                ' Only look at lines with text
                        If FileLines(Lp).Substring(0, 1) <> ";" Then                                        ' Ignore comments
                            If FoundSection = True And FileLines(Lp).Substring(0, 1).Trim = "[" Then        ' We found the section but no key found so add
                                If FileLines(Lp - 1).Trim = "" Then
                                    CancelBlank = 0
                                    Lp = Lp - 1 ' Found a blank before, will use that slot first
                                End If
                                ReDim Preserve FileLines(FileLines.Length + CancelBlank)                    ' Add one or two more elements to the array (new key + a blank line)
                                For RestofLoop As Integer = FileLines.Length - 1 To Lp Step -1
                                    FileLines(RestofLoop) = FileLines(RestofLoop - 1 - CancelBlank)         ' Shift the array up one or two elements depending on blank line
                                Next
                                FileLines(Lp) = Key + " = " + WriteVal                                      ' Write the new value to the spare slot
                                FileLines(Lp + 1) = ""                                                      ' insert blank line
                                Return True
                            End If
                            If FileLines(Lp).Trim.ToLower = "[" + Section.ToLower + "]" Then FoundSection = True
                            If FoundSection = False Then Continue For ' Still looking for the section
                            EquPos = FileLines(Lp).IndexOf("=")                             ' Find the position of the '=' to split key & value
                            If EquPos > 1 And FoundSection = True Then                      ' Minimum 1 character as key else we didn't find a valid line
                                KeyName = FileLines(Lp).Substring(0, EquPos - 1).Trim.ToLower
                                If KeyName = Key.ToLower Then
                                    FileLines(Lp) = FileLines(Lp).Substring(0, EquPos - 1).Trim + " = " + WriteVal           ' Replace the line in the cache with the new one
                                    Return True
                                End If
                            End If
                        End If
                    End If
                Catch ex As Exception                     ' Ignore lines that throw errors (eg. no characters)
                End Try
            Next                            ' No section found, so create a new section & add entry
            If FileLines(FileLines.Length - 1).Trim = "" Then CancelBlank = 0 ' No, found a blank before, will use that slot
            If FoundSection = True Then     ' Found the section but no key, and it was the last section
                ReDim Preserve FileLines(FileLines.Length + CancelBlank - 1)            ' Add one more element to the array
            Else                            ' Didn't find either the section or the key, so create a new section
                ReDim Preserve FileLines(FileLines.Length + CancelBlank + 1)                                         ' Increase the size of the array, write over any blank line at the end of the file
                FileLines(FileLines.Length - 3) = ""
                FileLines(FileLines.Length - 2) = "[" + Section + "]"
            End If
            FileLines(FileLines.Length - 1) = Key + " = " + WriteVal
        End SyncLock
        Return True
    End Function

    ' Write the ini file string array cache to a new ini file
    Public Function WriteIniSettings() As Boolean
        SyncLock FileLock                                                                           ' Ensure no other thread is trying to read/write
            Try
                Dim writer As IO.TextWriter = IO.File.CreateText(FileName + ".tmp")                       ' Temporary file
                For WrLp = 0 To FileLines.Length - 1                                                ' Spin through array writing elements to lines in the file
                    writer.WriteLine(FileLines(WrLp))
                Next
                writer.Close()
                My.Computer.FileSystem.MoveFile(FileName + ".tmp", FileName + ".ini", True)         ' Move the temporary file to the ini file
                Return True                                                                         ' All OK
            Catch ex As Exception
                Return False                                                                        ' Bad stuff happened
            End Try
        End SyncLock
    End Function

End Class

