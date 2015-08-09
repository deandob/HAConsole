Imports System.IO

' System Utilities
Public Module HAUtils

#Region "Ini File Management"

    Private LastTimeSysErrorLogged As Date
    Private NumSysErrors As Integer

#End Region
    '##############################################################################################################################################################################
#Region "Script Management"

    ' Wrapper to control methods exposed to scripts
    Public Class ServerFunctions
        Public Function GetState(Category As String, ClassName As String, Instance As String, Scope As String) As String
            GetState = Scope + " " + Instance
        End Function
        Public Function CreateMessage(ClassName As String, Func As Byte, Level As Byte, Instance As String, Scope As String, Data As String, Optional CatNum As String = "") As Commons.Structures.HAMessageStruc
            Return HS.CreateMessage(ClassName, Func, Level, Instance, Scope, Data, CatNum)
        End Function
    End Class

    Public Class Scripting
        ' Need to run in a separate app domain for isolation.....
        'http://www.codeproject.com/Answers/169630/code-generating-using-CodeDom.aspx#answer5
        'http://www.codeproject.com/Answers/168158/Create-WPF-Application-that-uses-Reloadable-Plugin.aspx#answer1
        'http://www.techtalkz.com/vb-net/126333-codedom-generateinmemory-memory-leak.html

        'Public Delegate Function DynamicDelegate(Param1 As String, Param2 As String) As Boolean ' this delegate must match the signature in script header declarations

        Private _CompileErrors As CodeDom.Compiler.CompilerErrorCollection
        'Private _CompiledAssy() As System.Reflection.Assembly
        Private _CompileErr As String

        Private Assys As List(Of AssyInfo) = New List(Of AssyInfo)
        Private Structure AssyInfo
            Dim Name As String
            Dim Description As String
            Dim Assy As System.Reflection.Assembly
        End Structure

        Public Sub SaveAss(AssName As String, AssDesc As String, ByRef Ass As System.Reflection.Assembly)         ' Pass byref so that we don't copy the assembly around
            Dim myAss As AssyInfo
            myAss.Name = AssName
            myAss.Description = AssDesc
            myAss.Assy = Ass
            Assys.Add(myAss)
        End Sub

        Public Function GetAss(AssName As String) As System.Reflection.Assembly
            Return Assys.Find(Function(Assy As AssyInfo)
                                  Return Assy.Name.ToUpper = AssName.ToUpper
                              End Function).Assy                ' Find the assembly with the same index as the assembly name
        End Function

        Public ReadOnly Property AssyCount As Integer
            Get
                Return Assys.Count
            End Get
        End Property

        Public Property CompileErr As String
            Get
                Return _CompileErr
            End Get
            Set(Value As String)
                _CompileErr = Value
            End Set
        End Property

        Public Sub New()
            MyBase.New()
        End Sub

        ' Compile using the codedom and language. Note compiled assemblies will use memory
        Public Function CompileScript(ScriptName As String, ScriptDesc As String, ScriptType As String, Script As String, Optional AssRefs() As String = Nothing) As Boolean

            Try
                Dim provider As System.CodeDom.Compiler.CodeDomProvider = Nothing

                Dim providerOptions = New Collections.Generic.Dictionary(Of String, String)
                providerOptions.Add("CompilerVersion", "v4.0")                                      ' Default is v2.0, update to v3.5
                Dim CompileParams As System.CodeDom.Compiler.CompilerParameters = New System.CodeDom.Compiler.CompilerParameters
                Dim Results As CodeDom.Compiler.CompilerResults
                Dim CompileErr As String = ""
                Dim AdjustLines As Integer = 0                                                  ' For SCR files the error lines have to be adjusted for the prepopulated header

                CompileParams.GenerateExecutable = False
                CompileParams.IncludeDebugInformation = False
                CompileParams.CompilerOptions = "/optionexplicit /optionstrict- /nowarn /optimize- /OptionInfer+"

                'CompileParams.TempFiles = New System.CodeDom.Compiler.TempFileCollection(System.IO.Path.GetTempPath(), False)
                CompileParams.GenerateInMemory = True      'Assembly is created in memory
                CompileParams.GenerateExecutable = False
                CompileParams.TreatWarningsAsErrors = False
                CompileParams.WarningLevel = 4
                CompileParams.IncludeDebugInformation = False

                'Put any dll references needed
                CompileParams.ReferencedAssemblies.Add("System.dll")
                If AssRefs IsNot Nothing Then CompileParams.ReferencedAssemblies.AddRange(AssRefs) ' Add any additional assemblies needed
                CompileParams.ReferencedAssemblies.Add(Reflection.Assembly.GetExecutingAssembly().Location)             ' Add my assembly so script can call methods in the host

                Select Case ScriptType
                    Case Is = "SCR"                                                             ' Simple VB scripts (no headers etc). Server object hosts functions from HAServer, return result in 'result' variable
                        'Dim Provider As Microsoft.VisualBasic.VBCodeProvider = New Microsoft.VisualBasic.VBCodeProvider(providerOptions)
                        CompileParams.ReferencedAssemblies.Add("Microsoft.VisualBasic.dll")
                        provider = New Microsoft.VisualBasic.VBCodeProvider(providerOptions)
                        Dim ScriptTopLines() As String = {"Imports Microsoft.visualbasic", _
                                                       "Imports System", _
                                                       "Imports " + GetType(HAUtils).Namespace, _
                                                       "Namespace " + ScriptName, _
                                                       "    Public Class ScriptClass", _
                                                       "        Public Shared Function Main(optional param1 As String = Nothing, optional param2 As String = Nothing, optional param3 as String = Nothing, optional param4 as String = Nothing) as String", _
                                                          "             Dim Server as new ServerFunctions", _
                                                       "            Dim result as String = """""}
                        Dim TopScript As String = Join(ScriptTopLines, vbLf)
                        Dim ScriptBotLines() As String = {"            Return result", _
                                                        "      End Function", _
                                                        "  End Class", _
                                                        "End Namespace"}
                        Dim BotScript As String = Join(ScriptBotLines, vbLf)
                        Script = TopScript + vbLf + Script + vbLf + BotScript        ' Add the script class/func declarations to the user script
                        Results = provider.CompileAssemblyFromSource(CompileParams, Script)
                        provider.Dispose()
                        AdjustLines = ScriptTopLines.Count
                    Case Is = "CS"                                                              ' Full C#
                        provider = New Microsoft.CSharp.CSharpCodeProvider(providerOptions)
                        Results = provider.CompileAssemblyFromSource(CompileParams, Script)
                        provider.Dispose()
                    Case Is = "VB"                                                              ' Full VB
                        CompileParams.ReferencedAssemblies.Add("Microsoft.VisualBasic.dll")
                        provider = New Microsoft.VisualBasic.VBCodeProvider(providerOptions)
                        Results = provider.CompileAssemblyFromSource(CompileParams, Script)
                        provider.Dispose()
                    Case Else
                        Me.CompileErr = "Script '" + ScriptName + "' not compiled. Scripts must be either .scr (simple VB scripts), .vb (Visual Basic .NET) or .cs (C# .NET)"
                        Return False
                End Select
                If Results.Errors.Count = 0 Then                                    'Any compiler errors or warnings?
                    SaveAss(ScriptName, ScriptDesc, Results.CompiledAssembly)                      ' Save assembly in the object for future ServiceState.RUNNING
                    Return True
                Else
                    Dim err As System.CodeDom.Compiler.CompilerError
                    Dim ErrMsg As String = "Script Compilation Error(s): "
                    For Each err In Results.Errors
                        ErrMsg = ErrMsg + vbCrLf + (String.Format("Line {0}, Col {1}: Error {2} - {3}", _
                            err.Line - AdjustLines, err.Column, err.ErrorNumber, err.ErrorText))          ' Line number is the script lines minus the autogen script headers (reflects the end user line number)
                    Next
                    Me.CompileErr = ErrMsg                              ' Save errors in the object for processing in the calling method
                    Return False
                End If
            Catch ex As Exception
                HandleSysErrors(False, "WARNING", "System error when compiling script '" + ScriptName + "'. Script not available for ServiceState.RUNNING.", ex)
                Me.CompileErr = "System error when compiling script '" + ScriptName + "'. Error: " + ex.ToString
                Return False
            End Try
        End Function

        ' Get the script file and pass to compile routing
        Public Function CompileScriptFile(ScriptName As String, ScriptDesc As String, Optional AssRefs() As String = Nothing) As Boolean
            Try
                Dim GetFiles As String() = Directory.GetFiles(HS.Scriptpath, ScriptName + ".*")              ' Get the script files corresponding to the file name
                If GetFiles.Count = 0 Then CompileErr = "Cannot find file '" + ScriptName + "' to compile." : Return False
                Dim ScriptFileName As String = GetFiles(0)                                      ' Always use the first file found (avoid using same file names)
                Dim sourceFile As System.IO.FileInfo = New System.IO.FileInfo(ScriptFileName)
                Dim ScriptType As String = sourceFile.Extension.ToUpper(Globalization.CultureInfo.InvariantCulture)
                If ScriptType <> "" Then
                    ScriptType = ScriptType.Substring(1)                       ' Remove the '.' from the extension
                    Dim FileScript As String = File.ReadAllText(ScriptFileName)                                             ' Serialize the script file
                    Return CompileScript(ScriptName, ScriptDesc, ScriptType, FileScript, AssRefs)
                Else
                    Return False                            ' Bad extension
                End If
            Catch ex As Exception
                HandleSysErrors(False, "WARNING", "System error when compiling script '" + ScriptName + "'. Script not available for ServiceState.RUNNING.", ex)
                Me.CompileErr = "Error loading script file  '" + ScriptName + "'. Error: " + ex.ToString
                Return False
            End Try
        End Function

        Public Function Run(ScriptName As String, Optional param1 As String = Nothing, Optional param2 As String = Nothing, Optional param3 As String = Nothing, Optional param4 As String = Nothing) As String
            Dim ExecInstance As Object = Nothing
            Dim RetObj As Object = Nothing
            Dim MethodInfo As Reflection.MethodInfo
            Dim InstType As Type
            Dim MethodParams As Object()
            Dim Reflectionflags As Reflection.BindingFlags = Reflection.BindingFlags.Public Or Reflection.BindingFlags.Static Or Reflection.BindingFlags.IgnoreCase

            Try
                'ExecInstance = Me.CompiledAssy.CreateInstance(ScriptName + ".ScriptClass")
                Dim myAss As System.Reflection.Assembly = GetAss(ScriptName)              ' Find the right assembly to run
                If myAss IsNot Nothing Then
                    Dim myType As Type = myAss.GetType(ScriptName + ".ScriptClass", False, True)            ' Don't throw an error and ignore case
                    If myType Is Nothing Then
                        MsgBox("Can't find Type '" + ScriptName + ".ScriptClass' in assembly. Can't run")               ' XXX WILL BLOCK ServiceState.RUNNING, NEED A BETTER WAY TO REFLECT ERROR
                        Return Nothing
                    End If
                    'ExecInstance = myAss.CreateInstance(ScriptName + ".ScriptClass")
                    ExecInstance = myAss.CreateInstance(myType.FullName)
                    InstType = ExecInstance.GetType
                    MethodInfo = InstType.GetMethod("Main", Reflectionflags)
                    If MethodInfo = Nothing Then
                        MsgBox("Can't find Method RunScript in script '" + ScriptName + " assembly. Can't run")               ' XXX WILL BLOCK ServiceState.RUNNING, NEED A BETTER WAY TO REFLECT ERROR
                        Return Nothing
                    End If
                    MethodParams = {param1, param2, param3, param4}
                    RetObj = MethodInfo.Invoke(ExecInstance, MethodParams)
                    'rslt = thisType.InvokeMember("Main", BindingFlags.Static Or BindingFlags.Public Or BindingFlags.InvokeMethod Or BindingFlags.IgnoreCase, Nothing, Nothing, args, Globalization.CultureInfo.InvariantCulture)
                    If Not RetObj Is Nothing Then Return CType(RetObj, String) ' Got a result, return it as a string
                End If
                Return Nothing                                  ' Either couldn't find the assembly or the script method called returned nothing
            Catch ex As Exception
                HandleSysErrors(False, "WARNING", "System error when ServiceState.RUNNING script '" + ScriptName + "'. Script aborted.", ex)
                Throw                      ' Pass error up to calling module (Could also load an error in the  object..............
                Return Nothing
            End Try
        End Function
    End Class

    ' Return all the file names for scripts in the scripts directory
    Public Function GetScriptFileNames() As String()
        Dim FilterFiles As List(Of String) = New List(Of String)
        Dim ScriptPath As String = "Scripts"
        If Not Directory.Exists(ScriptPath) Then
            Directory.CreateDirectory(ScriptPath)
            Return Nothing
        End If
        For Each FileName In Directory.GetFiles(ScriptPath, "*.SCR")
            FilterFiles.Add(Path.GetFileNameWithoutExtension(FileName))
        Next
        For Each FileName In Directory.GetFiles(ScriptPath, "*.VB")
            FilterFiles.Add(Path.GetFileNameWithoutExtension(FileName))
        Next
        For Each FileName In Directory.GetFiles(ScriptPath, "*.CS")
            FilterFiles.Add(Path.GetFileNameWithoutExtension(FileName))
        Next
        Return FilterFiles.ToArray
    End Function

#End Region
    '##############################################################################################################################################################################
#Region "Misc"

    ' Write a message to the console
    Public Sub WriteConsole(DisplayDebug As Boolean, ConsoleStr As String, <Runtime.CompilerServices.CallerMemberName> Optional CallerName As String = "")
        If DisplayDebug = True Or HAConsole.DebugMode = True Then Console.WriteLine(Date.Now.ToString("HH:mm:ss.fff") + " [" + CallerName + "]" + vbTab + ConsoleStr) ' Append a Date/Time stamp
    End Sub

    ' Common routine for handling errors generated by the UI or fatal errors. The end user is expected to interact with the messagebox as the console will be halted.
    Public Sub HandleSysErrors(ExitConsole As Boolean, ErrorSeverity As String, ErrorMessage As String, ex As Exception)
        If NumSysErrors < 5 Then
            IO.File.AppendAllText(HAServices.HomeServices.ERROR_LOG_FILE_NAME, DateTime.Now.ToString + " " + ErrorSeverity.ToUpper + ": " + ErrorMessage + ". Error Details: " + ex.ToString.Replace(vbCr, "").Replace(vbLf, "") + vbCrLf)
            WriteConsole(True, ErrorSeverity.ToUpper + " " + ErrorMessage + ". Error details below." + vbCrLf + ex.ToString)
            'HAServices.CreateMessage(.........)            XXXXXXX LOG TO CONSOLE THE ERROR, NEED TO PASS THE MESSAGE STRUCTURE + DONT LOG ERRORS 
        End If
        If LastTimeSysErrorLogged.Second = DateTime.Now.Second Then NumSysErrors = NumSysErrors + 1 Else NumSysErrors = 0 ' Don't jam the log file if many errors are raised concurrently.
        LastTimeSysErrorLogged = DateTime.Now
        ShutConsole(1)                                                           ' Exit the application with bad exit code
        'If ExitConsole Then Environment.Exit(1) ' Exit code is not 0
    End Sub

    ' Convert to Javascript (Unix) time
    Public Function ToJSTime(Ticks As Long) As Long
        Return CLng((Ticks - Commons.HAConst.unixEpoc) / Commons.HAConst.pcTicks)
    End Function

    ' Convert from Javascript time
    Public Function FromJSTIme(Ticks As Long) As Long
        Return Ticks * Commons.HAConst.pcTicks + Commons.HAConst.unixEpoc
    End Function

    ' Helper function that returns the content of a HTTP GET in a string list
    Public Function HTTPGet(URL As String) As List(Of String)
        Dim RetList As List(Of String) = New List(Of String)
        Dim GetURL As Net.WebRequest
        GetURL = Net.WebRequest.Create(URL)
        Dim objStream As Stream
        objStream = GetURL.GetResponse().GetResponseStream

        Dim objReader As New StreamReader(objStream)

        Dim HTTPLines As String = objReader.ReadLine.ToString
        Do While HTTPLines IsNot Nothing
            'MsgBox(HTTPLines)
            RetList.Add(HTTPLines)
            HTTPLines = objReader.ReadLine
        Loop
        Return RetList
    End Function

#End Region
    '##############################################################################################################################################################################
#Region "Sunrise Sunset"

    Public Class SunriseAndSunset

        ' Convert from a decimal representation of longitute or latitude to degrees and minutes
        Public Function ConverttoDegMin(LongLat As String) As String
            Dim LongDegrees As Integer = CInt(LongLat.Trim)                                             ' Degrees is the integer portion of the number
            Dim LongMins As Integer = Math.Abs(CInt((CSng(LongLat.Trim) - CSng(LongDegrees)) * 60))               ' Minutes is the fraction * 60
            Return LongDegrees.ToString + "," + LongMins.ToString
        End Function

        ' Convert from a Degree Minutes string representation of longitute or latitude to decimal
        Public Function ConvertfromDegMin(LongLat As String) As Double
            Dim LongDec As Double = CDbl(LongLat.Substring(0, LongLat.IndexOf(",")))                        ' Degrees is the first number in the string
            Dim LongFraction As Double = CDbl(LongLat.Substring(LongLat.IndexOf(",") + 1)) / 60               ' Minutes is the second
            Return LongDec + LongFraction
        End Function

        ' Daylight Savings offset calculation, returning number of minutes to add when in daylight savings time
        Public Function DaylightSavingsOffset(CalcDate As DateTime) As Integer
            Dim LocalTZ As TimeZone = TimeZone.CurrentTimeZone
            Dim MinOffset As Globalization.DaylightTime = TimeZone.CurrentTimeZone.GetDaylightChanges(CalcDate.Year)
            Return CInt(LocalTZ.IsDaylightSavingTime(CalcDate)) * CInt(MinOffset.Delta.TotalMinutes)
        End Function

        Public Sub CalculateSolarTimes(dLatitude As Double, dLongitude As Double, _
                                       dtDesiredDate As Date, ByRef dtSunrise As Date, _
                                       ByRef dtSolarNoon As Date, ByRef dtSunset As Date)
            Dim lDaySavings As Long
            Dim dGammaSolarNoon As Double
            Dim dTimeGMT As Double
            Dim dSolarNoonGMT As Double
            Dim dTimeLST As Double
            Dim dSolarNoonLST As Double
            Dim dSunsetTimeGMT As Double
            Dim dSunsetTimeLST As Double
            Dim tsTimeZone As TimeSpan
            Dim dZone As Double
            Dim dEquationOfTime As Double
            Dim dSolarDeclination As Double

            If dtDesiredDate.IsDaylightSavingTime Then
                lDaySavings = 60
            Else
                lDaySavings = 0
            End If
            tsTimeZone = dtDesiredDate.ToUniversalTime.Subtract(dtDesiredDate)
            dZone = tsTimeZone.TotalHours
            If dtDesiredDate.IsDaylightSavingTime Then dZone += 1

            If dLatitude >= -90 And dLatitude < -89.8 Then
                dLatitude = -89.8
            End If
            If dLatitude <= 90 And dLatitude > 89.8 Then
                dLatitude = 89.8
            End If

            ' Calculate the time of sunrise
            dGammaSolarNoon = CalculateGamma2(dtDesiredDate.DayOfYear, CLng(12 + (dLongitude / 15)))
            dEquationOfTime = CalculatedEquationOfTime(dGammaSolarNoon)
            dSolarDeclination = CalculateSolarDeclination(dGammaSolarNoon)

            dTimeGMT = CalculateSunriseGMT(dtDesiredDate.DayOfYear, dLatitude, dLongitude)

            dSolarNoonGMT = CalculateSolarNoonGMT(dtDesiredDate.DayOfYear, dLongitude)

            dTimeLST = dTimeGMT - (60 * dZone) + lDaySavings
            dtSunrise = dtDesiredDate.Date.AddMinutes(CInt(dTimeLST))

            'Calculate solar noon
            dSolarNoonLST = dSolarNoonGMT - (60 * dZone) + lDaySavings
            dtSolarNoon = dtDesiredDate.Date.AddMinutes(dSolarNoonLST)

            'Calculate  sunset
            dSunsetTimeGMT = CalculateSunsetGMT(dtDesiredDate.DayOfYear, dLatitude, dLongitude)
            dSunsetTimeLST = dSunsetTimeGMT - (60 * dZone) + lDaySavings
            dtSunset = dtDesiredDate.Date.AddSeconds(dSunsetTimeLST * 60)

        End Sub

        Private Function RadiansToDegrees(dAndgleInRadians As Double) As Double
            Return 180 * dAndgleInRadians / Math.PI
        End Function

        Private Function DegreesToRadians(dAngleInDegrees As Double) As Double
            Return Math.PI * dAngleInDegrees / 180
        End Function

        Private Function CalculateGamma(nJulianDay As Long) As Double
            Return (2 * Math.PI / 365) * (nJulianDay - 1)
        End Function

        Private Function CalculateGamma2(nJulianDay As Long, lHour As Long) As Double
            Return (2 * Math.PI / 365) * (nJulianDay - 1 + (lHour / 24))
        End Function

        Private Function CalculatedEquationOfTime(dGamma As Double) As Double
            Return (229.18 * (0.000075 + 0.001568 * Math.Cos(dGamma) - 0.032077 * Math.Sin(dGamma) - _
                    0.014615 * Math.Cos(2 * dGamma) - 0.040849 * Math.Sin(2 * dGamma)))
        End Function

        Private Function CalculateSolarDeclination(dGamma As Double) As Double
            Return (0.006918 - 0.399912 * Math.Cos(dGamma) + 0.070257 * Math.Sin(dGamma) - 0.006758 * _
                    Math.Cos(2 * dGamma) + 0.000907 * Math.Sin(2 * dGamma))
        End Function

        Private Function CalculateHourAngle(dLatitude As Double, dSolarDeclination As Double, _
                                            bIsTime As Boolean) As Double

            Dim dRadianLatitude As Double
            Dim dHourAngle As Double

            dRadianLatitude = DegreesToRadians(dLatitude)

            If bIsTime Then
                dHourAngle = (Math.Acos(Math.Cos(DegreesToRadians(90.833)) / (Math.Cos(dRadianLatitude) * _
                   Math.Cos(dSolarDeclination)) - Math.Tan(dRadianLatitude) * Math.Tan(dSolarDeclination)))

            Else
                dHourAngle = -(Math.Acos(Math.Cos(DegreesToRadians(90.833)) / (Math.Cos(dRadianLatitude) * _
                   Math.Cos(dSolarDeclination)) - Math.Tan(dRadianLatitude) * Math.Tan(dSolarDeclination)))

            End If

            Return dHourAngle

        End Function

        Private Function CalculateSunriseGMT(lJulianDay As Long, dLatitude As Double, _
                                             dLongitude As Double) As Double

            Dim dGamma As Double
            Dim dEquationOfTime As Double
            Dim dSolarDeclination As Double
            Dim dHourAngle As Double
            Dim dDelta As Double
            Dim dTimeDifference As Double
            Dim dTimeGMT As Double
            Dim dGammaSunrise As Double

            dGamma = CalculateGamma(lJulianDay)
            dEquationOfTime = CalculatedEquationOfTime(dGamma)
            dSolarDeclination = CalculateSolarDeclination(dGamma)
            dHourAngle = CalculateHourAngle(dLatitude, dSolarDeclination, True)
            dDelta = dLongitude - RadiansToDegrees(dHourAngle)
            dTimeDifference = 4 * dDelta
            dTimeGMT = 720 + dTimeDifference - dEquationOfTime

            dGammaSunrise = CalculateGamma2(lJulianDay, CLng(dTimeGMT / 60))
            dEquationOfTime = CalculatedEquationOfTime(dGammaSunrise)
            dSolarDeclination = CalculateSolarDeclination(dGammaSunrise)
            dHourAngle = CalculateHourAngle(dLatitude, dSolarDeclination, True)
            dDelta = dLongitude - RadiansToDegrees(dHourAngle)
            dTimeDifference = 4 * dDelta
            dTimeGMT = 720 + dTimeDifference - dEquationOfTime

            Return dTimeGMT

        End Function

        Private Function CalculateSolarNoonGMT(nJulianDay As Long, nLongitude As Double) As Double

            Dim dGammaSolarNoon As Double
            Dim dEquationOfTime As Double
            Dim dSolarNoonDeclination As Double
            Dim dSolarNoonGMT As Double

            dGammaSolarNoon = CalculateGamma2(nJulianDay, CLng(12 + (nLongitude / 15)))
            dEquationOfTime = CalculatedEquationOfTime(dGammaSolarNoon)
            dSolarNoonDeclination = CalculateSolarDeclination(dGammaSolarNoon)
            dSolarNoonGMT = 720 + (nLongitude * 4) - dEquationOfTime

            Return dSolarNoonGMT

        End Function

        Private Function CalculateSunsetGMT(nJulianDay As Long, nLatitude As Double, _
                                            nLongitude As Double) As Double


            Dim dGamma As Double
            Dim dEquationOfTime As Double
            Dim dSolarDeclination As Double
            Dim dHourAngle As Double
            Dim dDelta As Double
            Dim dTimeDiff As Double
            Dim dSetTimeGMT As Double
            Dim dGammaSunset As Double

            dGamma = CalculateGamma(nJulianDay + 1)
            dEquationOfTime = CalculatedEquationOfTime(dGamma)
            dSolarDeclination = CalculateSolarDeclination(dGamma)
            dHourAngle = CalculateHourAngle(nLatitude, dSolarDeclination, False)
            dDelta = nLongitude - RadiansToDegrees(dHourAngle)
            dTimeDiff = 4 * dDelta
            dSetTimeGMT = 720 + dTimeDiff - dEquationOfTime

            dGammaSunset = CalculateGamma2(nJulianDay, CLng(dSetTimeGMT / 60))
            dEquationOfTime = CalculatedEquationOfTime(dGammaSunset)

            dSolarDeclination = CalculateSolarDeclination(dGammaSunset)

            dHourAngle = CalculateHourAngle(nLatitude, dSolarDeclination, False)
            dDelta = nLongitude - RadiansToDegrees(dHourAngle)
            dTimeDiff = 4 * dDelta
            dSetTimeGMT = 720 + dTimeDiff - dEquationOfTime

            Return dSetTimeGMT

        End Function

    End Class

#End Region
End Module
