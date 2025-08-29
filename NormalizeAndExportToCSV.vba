Sub NormalizeAndExportToCSV()
    On Error GoTo FatalError

    Dim inputWorkbook As Workbook
    Dim inputFilePath As String
    Dim currentWorkbook As Workbook
    Dim ws As Worksheet
    Dim dbTable As ListObject
    Dim lastCol As Long
    Dim i As Long
    Dim header As String
    Dim snakeHeader As String
    Dim baseName As String
    Dim savePath As String
    Dim todayDate As String
    Dim fileName As String
    Dim csvFilePath As String
    Dim fd As FileDialog
    Dim tempWS As Worksheet
    Dim row As ListRow
    Dim duplicateRemoved As Boolean

    Set currentWorkbook = ThisWorkbook

    Set dbTable = Nothing
    For Each ws In currentWorkbook.Worksheets
        For Each dbTable In ws.ListObjects
            If dbTable.Name = "DbPrepper" Then Exit For
        Next dbTable
        If Not dbTable Is Nothing Then Exit For
    Next ws

    If dbTable Is Nothing Then
        MsgBox "Table 'DbPrepper' not found in current workbook.", vbCritical
        Exit Sub
    End If

    For Each row In dbTable.ListRows
        Select Case LCase(Trim(row.Range(1, 1).Value))
            Case "base name"
                baseName = Trim(row.Range(1, 2).Value)
            Case "file save path"
                savePath = Trim(row.Range(1, 2).Value)
        End Select
    Next row

    If baseName = "" Or savePath = "" Then
        MsgBox "Missing 'Base Name' or 'File Save Path' in DbPrepper table.", vbCritical
        Exit Sub
    End If

    Set fd = Application.FileDialog(msoFileDialogFilePicker)
    fd.Title = "Select Excel File to Process"
    fd.Filters.Clear
    fd.Filters.Add "Excel Files", "*.xls; *.xlsx; *.xlsm"
    If fd.Show <> -1 Then Exit Sub
    inputFilePath = fd.SelectedItems(1)

    Set inputWorkbook = Workbooks.Open(inputFilePath)

    Set ws = inputWorkbook.Sheets(1)

    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column

    For i = 1 To lastCol
        header = ws.Cells(1, i).Value
        snakeHeader = NormalizeToSnakeCase(header)
        ws.Cells(1, i).Value = snakeHeader
    Next i


    Dim colSelector As New ColumnSelector
    Dim headers() As String
    ReDim headers(1 To lastCol)

    For i = 1 To lastCol
        headers(i) = ws.Cells(1, i).Value
    Next i

    colSelector.InitializeHeaders headers
    colSelector.Show

    If colSelector.Cancelled Then
        MsgBox "Operation cancelled by user.", vbInformation
        inputWorkbook.Close False
        Exit Sub
    End If

    Dim keepCols As Collection
    Set keepCols = colSelector.SelectedHeaders

    For i = lastCol To 1 Step -1
        If Not HeaderInCollection(ws.Cells(1, i).Value, keepCols) Then
            ws.Columns(i).Delete
        End If
    Next i
    ws.Copy
    Set tempWS = ActiveWorkbook.Sheets(1)

    On Error Resume Next
    tempWS.UsedRange.RemoveDuplicates Columns:=Application.Transpose(Evaluate("ROW(1:" & lastCol & ")")), header:=xlYes
    If Err.Number <> 0 Then
        Err.Clear
    End If
    On Error GoTo FatalError

    todayDate = Format(Date, "dd_mm_yyyy")
    fileName = baseName & "_" & todayDate & ".csv"
    csvFilePath = savePath & "\" & fileName

    Application.DisplayAlerts = False
    ActiveWorkbook.SaveAs fileName:=csvFilePath, FileFormat:=xlCSV
    ActiveWorkbook.Close SaveChanges:=False
    Application.DisplayAlerts = True

    inputWorkbook.Close False

    MsgBox "CSV exported successfully to: " & csvFilePath
    Exit Sub

FatalError:
    MsgBox "An unexpected error occurred: " & Err.Description, vbCritical
    On Error Resume Next
    If Not inputWorkbook Is Nothing Then inputWorkbook.Close False
    If Not ActiveWorkbook Is Nothing Then ActiveWorkbook.Close False
    Application.DisplayAlerts = True
End Sub

Function NormalizeToSnakeCase(header As String) As String
    Dim temp As String
    temp = LCase(header)
    temp = Replace(temp, "[", "_")
    temp = Replace(temp, "]", "")
    temp = Replace(temp, " ", "_")
    temp = Replace(temp, "#", "number")
    temp = Replace(temp, "-", "_")
    temp = Replace(temp, "/", "_")
    temp = Replace(temp, ".", "_")
    Do While InStr(temp, "__") > 0
        temp = Replace(temp, "__", "_")
    Loop
    NormalizeToSnakeCase = temp
End Function

