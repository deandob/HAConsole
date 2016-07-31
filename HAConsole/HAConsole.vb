Imports Commons

' NuGet Package: System.Data.SQLite

' Compiled for ANYCPU (will run x64 on 64 bit OS), Option Strict is ON in the compiler settings for all projects
' Can't use the lib directory for the dlls used in the main project (HAConsole) as they need to exist in the GAC or in the same directory as the EXE. But for the Common and plugins, any DLLS these projects, set the specific reference properties to Copy Local = False to avoid copying those over
' Needs to run as administrator for the HTTP server to work

' Console application that handles all the home automation services
Module HAConsole
    ' Capture application shutdown events. Will not port to MONO
    Private Declare Function SetConsoleCtrlHandler Lib "kernel32.dll" (ByVal Handler As ShutHandler, ByVal Add As Boolean) As Boolean
    Private Delegate Function ShutHandler(ByVal CtrlType As HAConst.ShutTypes) As Boolean               ' Win32 DLL function to call when shutting down
    Private CtrlHandler As ShutHandler                                                                  ' Needed as static so that it is not garbage collected so that the handler knows where to call when shutting down

    Public HomeNet As New HANetwork.NetServices
    Public HS As New HAServices.HomeServices

    'Public HAAuto As New Automation

    Public Isx64 As Boolean = (IntPtr.Size = 8)                                                         ' ServiceState.RUNNING on x64 or x32

    'TODO: Archive log frequency in timer routine
    'TODO: IMplement network scope for messages
    'TODO: Error management
    'TODO: Message Security 
    'TODO: Shutdown sync so that remaining messages are processed
    'TODO: Date, Time, Network and Category filters, when active and selecting column filter, preload menustrip with the active values
    'TODO: Plugins turned on & off by message as well as autostart on startup
    'TODO: Archive log for the same day - crash as the file will be overwritten
    ' TODO: SYSERR routine needs to log the errors to the console
    'TODO: cache message states, update MatchState routine in automation to read cache for state matches in triggers
    'TODO: Put scipts and plugins into a different appdomain
    'TODO: Make INI file management only in HAServices
    'TODO: Update the automation UI when the underlying data is changed (eg. when a oneoff checks the event enable checkbox) 
    'TODO: Memory leaks, use 'using' to dispose better
    'TODO: Change the location of the data files (eg. where the client files are found)

    'http://support.microsoft.com/kb/317421
    'http://stackoverflow.com/questions/3447668/windows-service 

    ' Bug: Dates in the console screen are not automatically updated when format string changed
    ' BUg: Archive log fails with file closed errors
    ' BUG: Occassionally on startup the form is pushed to the background for 3 seconds after splashscreen disappears
    ' Bug: When the database code crashes, the shutdown isn't clean and it takes 1/2 second to close, probably waiting for the DB queue to flush and it never does
    ' BUG: longitude/lat error displayed occassionally on startup, possibly as form values have not rendered or loaded yet
    ' Bug: When the application crashes, the strings in the date and time are lost. Need to save them in a variable not in the UI

    ' superwebsocket . NET/Mono server http://superwebsocket.codeplex.com/, client is Websockets4net (client only, mono) http://websocket4net.codeplex.com/, 

    Public DebugMode As Boolean = False

    Sub Main()
        Dim args As String() = System.Environment.GetCommandLineArgs()
        Dim cki As ConsoleKeyInfo

        CtrlHandler = New ShutHandler(AddressOf CloseHandler)                   ' Setup shutdown routine
        SetConsoleCtrlHandler(CtrlHandler, True)                                ' Pass the shutdown routine to the Win32 DLL

        InitConsole(args)
        WriteConsole(True, "HA Console startup completed. Press 'X' to exit console.")

        While True                                                              ' Block waiting on key input, looping until 'X' is pressed, or console shutdown (shutdown caught by closehandler
            cki = Console.ReadKey(True)                                         ' Start a console read operation. Do not display the input.
            Select Case cki.Key
                Case Is = ConsoleKey.X                                          ' Exit
                    Exit While
                Case Is = ConsoleKey.C                                          ' Clear console screen
                    Console.Clear()
                Case Is = ConsoleKey.D                                          ' Debug mode
                    DebugMode = Not DebugMode
                    WriteConsole(True, "Debug mode: " + DebugMode.ToString)
                Case Is = ConsoleKey.S
                    ListStateStore()
            End Select
        End While
        ShutConsole(HAConst.ExitCodes.OK)                                       ' Console shutting down, cleanup before exit
    End Sub

    Public Sub ListStateStore()
        WriteConsole(True, "Listing StateStore...")
        For Each key In HS.HAStateStore.Keys
            WriteConsole(True, "Key:" + key.Category + "\" + key.ClassName + "\" + key.Instance + " (scope: " + key.Scope + "), Value: " + HS.HAStateStore(key))
        Next
    End Sub


    ' Start up the console. Command line arguments passed in args()
    Public Sub InitConsole(args() As String)
        HS.Init()                                                       ' Start general automation services
        HomeNet.Init()                                                          ' Start network services
    End Sub

    ' Handle any of the shutdown functions (eg. clicking on the close window). DOES NOT RESPOND TO SYSTEM SHUTDOWN EVENTS
    Public Function CloseHandler(ByVal ctrlType As HAConst.ShutTypes) As Boolean
        ShutConsole(HAConst.ExitCodes.OK)
        Return True
    End Function

    ' Code to run before exiting
    Public Sub Cleanup()
        WriteConsole(True, "Shutting down HA Console...")
        HomeNet.Stop()
        HS.StopServer()
        'System.Threading.Thread.Sleep(600)                              ' Short wait for everthing to finish
    End Sub

    ' Handles Ctrl-C allowing cleanup before exiting
    Public Sub HandleCtrlC(ByVal sender As Object, ByVal args As ConsoleCancelEventArgs)
        ShutConsole(HAConst.ExitCodes.OK)
        'args.Cancel = True          ' Stop Ctrl-C from exiting
    End Sub

    ' Final routine before exiting, run cleanup and exit with an errorcode if we are called due to a fatal error
    Public Sub ShutConsole(ExitCode As Integer)
        Cleanup()
        If ExitCode <> 0 Then
            WriteConsole(True, "Fatal errors occurred - Server stopped. Press any key to exit console.")
            Console.ReadKey(True)
        End If
        Environment.Exit(ExitCode)
    End Sub

End Module
