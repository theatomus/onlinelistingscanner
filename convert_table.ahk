#Requires AutoHotkey v2.0

; CapsLock pause functionality
CheckPause() {
    while (GetKeyState("CapsLock", "T")) {
        Sleep 100
    }
}

; Create directories if they don't exist
outputDir := A_ScriptDir . "\item_contents"
if (!DirExist(outputDir))
    DirCreate(outputDir)

inputDir := A_ScriptDir . "\table_data"
if (!DirExist(inputDir))
    DirCreate(inputDir)

if (A_Args.Length > 0) {
    ; Process single item if argument provided
    ProcessFile(A_Args[1])
} else {
    ; Process all files if no argument
    Loop Files, inputDir . "\*_table.raw" {
        itemNumber := RegExReplace(A_LoopFileName, "_table\.raw$", "")
        ProcessFile(itemNumber)
    }
}

RegExMatchAll(haystack, needle) {
    matches := []
    pos := 1
    while ((pos := RegExMatch(haystack, needle, &match, pos))) {
        matches.Push(match)
        pos += match.Len
    }
    return matches
}

ProcessFile(itemNumber) {
    CheckPause()
    inputFile := A_ScriptDir . "\table_data\" . itemNumber . "_table.raw"
    if (!FileExist(inputFile)) {
        ExitApp
    }

    outputFile := outputDir . "\" . itemNumber . "_description.txt"
    
    try {
        CheckPause()
        tableData := FileRead(inputFile)
    } catch Error as err {
        return
    }

    ; Handle HTML boundary markers if present
    startHTML := RegExMatch(tableData, "StartHTML:(\d+)", &startMatch)
    endHTML := RegExMatch(tableData, "EndHTML:(\d+)", &endMatch)

    if (startHTML && endHTML) {
        startPos := startMatch[1]
        endPos := endMatch[1]
        tableData := SubStr(tableData, startPos, endPos - startPos)
    }

    ; Find target tables
    validTables := FindTargetTables(tableData)
    
    ; Process ALL valid tables and combine entries
    allEntries := []
    masterHeaders := []  ; Initialize as empty array
    
    if (validTables.Length > 0) {
        for index, table in validTables {
            ; Extract headers from first table or reuse existing headers
            entries := ExtractTableHTMLWithHeaders(table.content, &masterHeaders, index = 1)
            for entry in entries {
                allEntries.Push(entry)
            }
        }
    } else {
        return
    }

    if (allEntries.Length == 0)
        return

    ; Merge entries that are actually fragments of a single vertical table
    allEntries := MergeEntriesIfNoConflicts(allEntries)

    ; Format the output
    output := FormatTableOutput(allEntries)

    ; Update the description file
    UpdateDescriptionFile(outputFile, output)
}

FindTargetTables(rawContent) {
    tables := []
    pos := 1
    
    while (pos := InStr(rawContent, "<table", false, pos)) {
        endPos := InStr(rawContent, "</table>", false, pos)
        if (!endPos)
            break
            
        tableLength := endPos - pos + 8
        tableContent := SubStr(rawContent, pos, tableLength)
        
        ; Score the table
        tableScore := ScoreTable(tableContent)
        
        tables.Push({
            pos: pos,
            score: tableScore.score,
            content: tableContent
        })
        
        pos := endPos + 1
    }
    
    ; Sort tables by score descending
    tables := Sort(tables, (a, b) => b.score - a.score)
    return tables
}

ScoreTable(content) {
    score := 0
    headerCount := 0  ; Initialize headerCount here
	
    ; Table markers (original method)
    markers := [
        {pattern: 'data-sheets-root="1"', points: 100, desc: "Google Sheets table"},
        {pattern: 'google-sheets-html-origin', points: 50, desc: "Google Sheets origin"},
        {pattern: 'class=".*table.*"', points: 20, desc: "Table class"},
        {pattern: '<colgroup>', points: 30, desc: "Column groups"},
        {pattern: '<thead>', points: 30, desc: "Table header section"},
        {pattern: '<tbody>', points: 30, desc: "Table body section"},
        {pattern: '_Make_.*_Model_', points: 80, desc: "Contains Make and Model headers"},
        {pattern: 'Make', points: 40, desc: "Contains Make header"},
        {pattern: 'Model', points: 40, desc: "Contains Model header"}
    ]
    
    ; Check for markers
    for marker in markers {
        if (RegExMatch(content, marker.pattern)) {
            score += marker.points
        }
    }
    
    ; Header patterns to check (original method)
    headers := [
        {pattern: '_Make_.*_Model_', points: 80},
        {pattern: '<td[^>]*>Make</td>', points: 70},
        {pattern: '<th[^>]*>Make</th>', points: 70},
        {pattern: '_CPU_.*_RAM_', points: 60},
        {pattern: 'Make.*Model.*CPU', points: 50}
    ]
    
    ; Check headers
    for header in headers {
        if (RegExMatch(content, header.pattern)) {
            score += header.points
        }
    }
    
    ; Check for common column headers, but less strict (original method)
    commonHeaders := ["Make", "Model", "CPU", "RAM", "Storage", "Notes", "Screen"]
    for header in commonHeaders {
        patterns := [
            "_" header "_",
            header "</td>",
            header ":",
            header "</th>"
        ]
        
        for pattern in patterns {
            if (RegExMatch(content, pattern)) {
                headerCount++
                break
            }
        }
    }
    
    if (headerCount > 0) {
        score += headerCount * 10
    }
    
    ; Check table structure (original method)
    if (RegExMatch(content, '<tr[^>]*>.*</tr>.*<tr[^>]*>.*</tr>')) {
        score += 40
    }
    
    if (RegExMatch(content, '<td[^>]*>.*</td>.*<td[^>]*>.*</td>')) {
        score += 40
    }
    
    ; New scoring logic for first-row headers (used as fallback)
    firstRowPos := RegExMatch(content, "<tr[^>]*>(.*?)</tr>", &firstRowMatch)
    if (firstRowPos) {
        firstRowContent := firstRowMatch[1]
        firstRowCells := RegExMatchAll(firstRowContent, "<t[dh][^>]*>(.*?)</t[dh]>")
        
        numColumns := firstRowCells.Length
        if (numColumns > 2) {
            score += (numColumns - 2) * 10  ; Bonus for multi-column tables
        }
        
        ; Extract and clean cell texts
        headerTexts := []
        for cell in firstRowCells {
            cellText := CleanHTML(cell[1], false, true)
            headerTexts.Push(cellText)
        }
        
        ; Prioritize tables with "Make" in the first row
        for text in headerTexts {
            if (text = "Make") {
                score += 1000  ; Large bonus ensures this table is selected if "Make" is present
                break
            }
        }
        
        ; Apply new scoring for other headers (less weight unless "Make" is absent)
        expectedHeaders := ["Make", "Model", "CPU", "RAM", "SSD", "Video Card", "Screen Size", "Notes"]
        for header in expectedHeaders {
            for text in headerTexts {
                if (text = header) {
                    score += 30  ; Standard points for each expected header
                    break
                }
            }
        }
    }
    
    return {score: score}
}

Sort(arr, compareFunc) {
    ; Simple bubble sort implementation
    n := arr.Length
    loop n {
        swapped := false
        loop n - A_Index {
            if (compareFunc(arr[A_Index], arr[A_Index + 1]) > 0) {
                temp := arr[A_Index]
                arr[A_Index] := arr[A_Index + 1]
                arr[A_Index + 1] := temp
                swapped := true
            }
        }
        if (!swapped)
            break
    }
    return arr
}

FormatTableOutput(entries) {
    output := ""
    for i, entry in entries {
        output .= "Entry " i ":`n"
        
        for pair in entry {
            field := pair[1]
            value := pair[2]
            
            if (field = "" || value = "") 
                continue
                
            ; Clean up underscore-separated field names
            field := RegExReplace(field, "_+", " ")
            
            colonSuffix := RegExMatch(field, ":+\s*$") ? " " : ": "
            
            if (field = "Notes") {
                output .= field ":`n"
                notes := StrSplit(value, "`n")
                for note in notes {
                    if (note := Trim(note)) {
                        ; Clean up extra spaces in notes
                        note := RegExReplace(note, "\s{2,}", " ")
                        output .= "  " note "`n"
                    }
                }
            } else if (InStr(value, "<br>")) {
                ; Handle multi-line values (like screen size)
                lines := StrSplit(value, "<br>")
                output .= field colonSuffix Trim(lines[1]) "`n"
                loop lines.Length - 1 {
                    nextLine := Trim(lines[A_Index + 1])
                    if (nextLine != "") {
                        ; Clean up any parenthesized values
                        if (RegExMatch(nextLine, "^\(.*\)$"))
                            output .= "  " nextLine "`n"
                        else
                            output .= "  " nextLine "`n"
                    }
                }
            } else {
                ; Clean up any extra spaces in values
                value := RegExReplace(value, "\s+", " ")
                value := Trim(value)
                output .= field colonSuffix value "`n"
            }
        }
        output .= "`n"
    }
    return output
}

FixEncoding(output) {
    ; Convert misrepresented degree symbols
    return StrReplace(output, "â€", "°")
}


UpdateDescriptionFile(outputFile, tableOutput) {
    if (!tableOutput)  ; Don't update if no table data
        return false
        
    try {
        if (FileExist(outputFile)) {
            ; Read existing content
            existingContent := FileRead(outputFile)
            
            ; Find section markers
            tablePos := InStr(existingContent, "=== TABLE DATA ===")
            descPos := InStr(existingContent, "=== ITEM DESCRIPTION ===")
            
            ; Create new content
            if (tablePos && descPos) {
                ; Replace existing table section
                newContent := SubStr(existingContent, 1, tablePos) . 
                             "=== TABLE DATA ===`n" . 
                             tableOutput . 
                             SubStr(existingContent, descPos - 1)
            } else if (descPos) {
                ; Insert table before description
                newContent := SubStr(existingContent, 1, descPos - 1) . 
                             "=== TABLE DATA ===`n" . 
                             tableOutput . 
                             SubStr(existingContent, descPos - 1)
            } else {
                ; Append all sections
                newContent := existingContent . 
                             "`n=== TABLE DATA ===`n" . 
                             tableOutput . 
                             "`n=== ITEM DESCRIPTION ===`n`n"
            }
            
            ; Apply the fix to the new content before writing
            newContent := FixEncoding(newContent)
            
            ; Write updated content
            FileDelete(outputFile)
            FileAppend(newContent, outputFile, "UTF-8")  ; Ensure UTF-8 encoding when writing
            
        } else {
            ; Create new file
            FileAppend(tableOutput . "`n=== ITEM DESCRIPTION ===`n`n", outputFile)
        }
        return true
    } catch Error as err {
        return false
    }
}

ExtractTableHTMLWithHeaders(rawData, &masterHeaders, isFirstTable := false) {
    entries := []
    pos := 1
    
    while (pos <= StrLen(rawData)) {
        startTablePos := InStr(rawData, "<table", false, pos)
        if (!startTablePos)
            break
            
        endTablePos := InStr(rawData, "</table>", false, startTablePos)
        if (!endTablePos)
            break
            
        tableContent := SubStr(rawData, startTablePos, endTablePos - startTablePos + 8)
        rows := RegExMatchAll(tableContent, "<tr[^>]*>(.*?)</tr>")
        
        if (rows.Length > 0) {
            ; Get first row cells to check structure
            firstRowCells := RegExMatchAll(rows[1][1], "<t[dh][^>]*>(.*?)</t[dh]>")
            
            if (firstRowCells.Length == 2) {
                ; Process as vertical table (2-column key-value pairs)
                entries.Push(ProcessVerticalTable(rows))
            } else {
                ; Process as horizontal table
                headers := []
                dataStartRow := 1
                
                ; Check if first row contains headers or if this is the first table
                isHeaderRow := false
                if (isFirstTable || masterHeaders.Length == 0) {
                    ; Look for header indicators in first row
                    firstRowContent := rows[1][1]
                    if (InStr(firstRowContent, "background-color") || 
                        InStr(firstRowContent, "font-weight: bold") ||
                        InStr(firstRowContent, "<th")) {
                        isHeaderRow := true
                    } else {
                        ; Check if content looks like headers
                        for cell in firstRowCells {
                            cellText := CleanHTML(cell[1], false, true)
                            if (RegExMatch(cellText, "^(Make|Model|Test Result|CPU|RAM|Storage|Network|Version|Missing|Serial|Notes|IMEI)")) {
                                isHeaderRow := true
                                break
                            }
                        }
                    }
                }
                
                if (isHeaderRow && (isFirstTable || masterHeaders.Length == 0)) {
                    ; Extract headers from first row
                    for cell in firstRowCells {
                        headerText := CleanHTML(cell[1], false, true)
                        headerText := RegExReplace(headerText, "^_+|_+$", "")
                        headerText := RegExReplace(headerText, ":+\s*$", "")
                        headerText := RegExReplace(headerText, "\s+", " ")
                        headerText := Trim(headerText)
                        if (headerText != "")
                            headers.Push(headerText)
                    }
                    masterHeaders := headers.Clone()  ; Store for reuse
                    dataStartRow := 2
                } else if (masterHeaders.Length > 0) {
                    ; Use previously extracted headers
                    headers := masterHeaders.Clone()
                    dataStartRow := 1
                } else {
                    ; Fallback to default headers
                    defaultHeaders := ["Make", "Model", "CPU", "RAM", "Storage", "Video Card", "Screen Size", "Notes"]
                    loop Min(firstRowCells.Length, defaultHeaders.Length) {
                        headers.Push(defaultHeaders[A_Index])
                    }
                    dataStartRow := 1
                }
                
                ; Process data rows
                if (headers.Length > 0) {
                    loop rows.Length - dataStartRow + 1 {
                        rowIndex := dataStartRow + A_Index - 1
                        if (rowIndex <= rows.Length) {
                            rowData := []
                            cells := RegExMatchAll(rows[rowIndex][1], "<td[^>]*>(.*?)</td>")
                            
                            ; Make sure we have enough cells for the headers
                            if (cells.Length >= headers.Length) {
                                loop headers.Length {
                                    label := headers[A_Index]
                                    value := ""
                                    if (A_Index <= cells.Length) {
                                        value := CleanHTML(cells[A_Index][1], label = "Notes", false)
                                    }
                                    if (label && value)
                                        rowData.Push([label, value])
                                }
                                
                                if (rowData.Length > 0)
                                    entries.Push(rowData)
                            }
                        }
                    }
                }
            }
        }
        
        pos := endTablePos + 8
    }
    
    return entries
}

MergeEntriesIfNoConflicts(entries) {
    try {
        if (entries.Length <= 1)
            return entries

        ; Build a merged map of label->value while checking for conflicts
        mergedPairs := []
        seen := Map()
        hasConflict := false

        for entry in entries {
            for pair in entry {
                label := pair[1]
                value := pair[2]
                if (label = "" || value = "")
                    continue

                ; Normalize label for comparison (ignore case and whitespace/underscores/colons)
                norm := StrLower(RegExReplace(label, "[\s_:+-]", ""))

                if (seen.Has(norm)) {
                    ; If the same field appears with a different value, we consider entries distinct
                    if (Trim(seen[norm]) != Trim(value)) {
                        hasConflict := true
                        break
                    }
                } else {
                    seen[norm] := value
                    mergedPairs.Push([label, value])
                }
            }
            if (hasConflict)
                break
        }

        ; Heuristic: only merge when there are no conflicts and there are enough unique fields to look like one item
        if (!hasConflict && mergedPairs.Length >= 5) {
            return [mergedPairs]
        }
    } catch Error as err {
        ; On any error, fall back to original entries
    }
    return entries
}

ProcessVerticalTable(rows) {
    entryData := []
    for row in rows {
        cells := RegExMatchAll(row[1], "<t[dh][^>]*>(.*?)</t[dh]>")
        if (cells.Length == 2) {
            label := CleanHTML(cells[1][1], false, true)
            value := CleanHTML(cells[2][1], false, false)  ; Assume no special treatment for Notes
            label := RegExReplace(label, ":\s*$", "")  ; Remove trailing colon
            if (label && value)
                entryData.Push([label, value])
        }
    }
    return entryData
}

CleanHTML(html, isNotes := false, cleanBR := false) {
    try {
        ; Remove leading/trailing underscores and asterisks
        if RegExMatch(html, "^[_*].*[_*]$")
            return RegExReplace(html, "^[_*]+|[_*]+$", "")
            
        if (InStr(html, "Yes</span>") || InStr(html, "N/A</span>") || InStr(html, "No</span>")) {
            if (!InStr(html, "</span><br>")) {
                html := RegExReplace(html, "</span>", " </span>")
            }
        }
        
        ; Handle line breaks based on context
        if (isNotes) {
            html := RegExReplace(html, "<br[^>]*>", "`n")
        } else if (cleanBR) {
            html := RegExReplace(html, "<br[^>]*>", " ")
        } else {
            html := RegExReplace(html, "<br[^>]*>\s*$", " ")
            html := RegExReplace(html, "<br[^>]*>(?=\S)", "##BR##")
        }
        
        ; Convert list items to newlines before removing other tags
        html := RegExReplace(html, "</li>", "</li>`n")
        
        html := RegExReplace(html, " | ", " ")
        html := RegExReplace(html, "&#?\w+;", "")
		
        ; Clean up HTML tags and entities
        html := RegExReplace(html, "<[^>]+>", "")
        html := RegExReplace(html, "&nbsp;|В|Â", " ")
        html := RegExReplace(html, "â„¢", "™")
        html := RegExReplace(html, "&amp;", "&")
        html := RegExReplace(html, "&quot;", chr(34))
        html := RegExReplace(html, "&apos;|&#39;", "'")
        html := RegExReplace(html, "&#\d+;", "")
        html := RegExReplace(html, "Â", " ")  ; Additional space character cleanup
        
        ; Restore line breaks if needed
        if (!cleanBR) {
            html := RegExReplace(html, "##BR##", "<br>")
        }
        
        ; Clean up whitespace
        html := RegExReplace(html, " {2,}", " ")
        html := RegExReplace(html, "^\s+|\s+$", "")
        html := RegExReplace(html, "(\r\n|\n\r|\r|\n)\s*(\r\n|\n\r|\r|\n)", "`n")  ; Normalize line endings
        
        return html
    } catch Error as err {
        return ""
    }
}