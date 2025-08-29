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

Function HeaderInCollection(header As String, col As Collection) As Boolean
    Dim item As Variant
    For Each item In col
        If item = header Then
            HeaderInCollection = True
            Exit Function
        End If
    Next item
    HeaderInCollection = False
End Function
