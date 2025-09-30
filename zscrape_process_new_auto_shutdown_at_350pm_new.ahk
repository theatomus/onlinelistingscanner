#Requires AutoHotkey v2.0
#SingleInstance Ignore ; AI note: prevent self-restart prompt; keep first instance running to avoid race with monitor
/*
eBay Listing Scanner and Validator - Enhanced Version with Detailed Logging
--------------------------------
Purpose:
- Scans both scheduled and active eBay listings
- Checks for missing or invalid SKUs
- Identifies duplicate titles with item number validation
- Enhanced reporting and monitoring with detailed logging
- On first run, scans all active listing pages
- On subsequent runs, only scans first page of active listings
- Auto-shuts down PC at 3:50 PM EST

Key Features:
- Human-like behavior with randomized delays
- Efficient tab management (opens new tab before closing old)
- Enhanced duplicate detection using item numbers (eliminates false positives)
- SKU validation (checks for 3-6 consecutive digits)
- Automatic file cleanup
- Error handling and reporting
- Real-time monitoring and statistics
- Hourly mini reports and daily summary reports
- DETAILED LOGGING for duplicate detection debugging
*/

; === Script Settings ===
SetWorkingDir A_ScriptDir
SendMode "Input"

; === Global Variables ===
global firstLoop := true
global isFirstTab := true
global isFirstActiveTab := true
global OUTPUT_FOLDER_DIR := A_ScriptDir . "\\eBayListingData"
 global LOGS_DIR := A_ScriptDir . "\\logs"
global SCANNING_LOGS_DIR := LOGS_DIR . "\\scanning"
global MONITORING_LOGS_DIR := LOGS_DIR . "\\monitoring"
global PROCESSING_LOGS_DIR := LOGS_DIR . "\\processing"
global INDIVIDUAL_LOGS_DIR := LOGS_DIR . "\\individual"
global MESSAGING_LOGS_DIR := LOGS_DIR . "\\messaging"
  global STATE_DIR := A_ScriptDir . "\\state"

; === SKU Handling Toggle ===
global USE_STANDARDIZED_SKU_HANDLING := true  ; Set to false to use original SKU handling

; === Ensure Log Directories Exist ===
EnsureLogDirectories() {
    if !DirExist(LOGS_DIR)
        DirCreate LOGS_DIR
    if !DirExist(SCANNING_LOGS_DIR)
        DirCreate SCANNING_LOGS_DIR
    if !DirExist(MONITORING_LOGS_DIR)
        DirCreate MONITORING_LOGS_DIR
    if !DirExist(PROCESSING_LOGS_DIR)
        DirCreate PROCESSING_LOGS_DIR
    if !DirExist(INDIVIDUAL_LOGS_DIR)
        DirCreate INDIVIDUAL_LOGS_DIR
    if !DirExist(MESSAGING_LOGS_DIR)
        DirCreate MESSAGING_LOGS_DIR
}

EnsureStateDirectory() {
    if !DirExist(STATE_DIR)
        DirCreate STATE_DIR
}

MigrateLegacyState() {
    ; Migrate legacy ignore lists from repo root to state dir if applicable
    legacyFiles := [
        "_ignore_list_scheduled.txt",
        "_ignore_list_active.txt"
    ]
    for legacy in legacyFiles {
        legacyPath := A_ScriptDir . "\\" . legacy
        newPath := STATE_DIR . "\\" . legacy
        if FileExist(legacyPath) {
            ; If new file doesn't exist or is empty, copy content
            needsCopy := false
            if !FileExist(newPath) {
                needsCopy := true
            } else {
                try {
                    content := FileRead(newPath)
                    if (Trim(content) = "")
                        needsCopy := true
                } catch as e {
                    needsCopy := true
                }
            }
            if (needsCopy) {
                try {
                    FileCopy(legacyPath, newPath, true)
                } catch as e {
                    ; ignore errors
                }
            }
        }
    }
}

; === NEW: Reporting Variables ===
global scriptStartTime := A_TickCount
global lastMiniReportTime := A_TickCount
global totalScannedToday := 0
global totalIssuesFound := 0
global prefixStats := Map()  ; Tracks counts by prefix (SF, etc)
global prefixIssues := Map() ; Tracks issues by prefix

; === NEW: Monitoring Integration ===
global monitorProcess := ""
; === Argument Flags ===
global NO_MONITOR := false
for arg in A_Args {
    if (arg = "--no-monitor") {
        NO_MONITOR := true
    }
}

; === NEW: Monitoring Functions ===
StartScanMonitor() {
    global monitorProcess
    ; Start the Python monitoring script
    try {
        ; Pass watchdog flags and this script path so monitor restarts zscrape without relaunching monitor
        ; Enable security watchdog with working window 09:00-15:30 and critical paths (repo root + sibling newsuite)
        repoRoot := A_ScriptDir
        siblingNewsuite := repoRoot . "\\..\\newsuite"
        ; AI note: launch Scan Monitor in GUI mode by default (no --daemon). Use --no-monitor on zscrape to suppress.
        ; If background/headless is desired, add --daemon in this command.
        ; Prevent duplicate Scan Monitor instances: if already running, skip starting a new one
        try {
            for proc in ComObjGet("winmgmts:\\root\\cimv2").ExecQuery("Select * from Win32_Process where Name='python.exe' or Name='pythonw.exe'") {
                cmdline := StrLower(proc.CommandLine)
                if InStr(cmdline, "scan_monitor.py") {
                    return
                }
            }
        } catch as _ {
        }
        cmd := 'python "scan_monitor.py" --zscrape-script "' . A_ScriptFullPath . '"'
        ; Do not force-enable watchdog here; it is off by default and can be toggled via GUI or hotkey
        cmd .= ' --watchdog-work-start 07:30 --watchdog-work-end 16:30'
        cmd .= ' --watchdog-path "' . repoRoot . '"'
        if DirExist(siblingNewsuite) {
            cmd .= ' --watchdog-path "' . siblingNewsuite . '"'
        }
        cmd .= ' --watchdog-control-file "' . repoRoot . '\\state\\watchdog_control.txt"'
        cmd .= ' --watchdog-status-file "' . repoRoot . '\\state\\watchdog_status.txt"'
        ; Ensure the monitor window is visible to the user
        Run(cmd, , , &monitorPid)
        monitorProcess := monitorPid
    } catch as e {
    }
}

StopScanMonitor() {
    global monitorProcess
    ; Stop the monitoring process
    if (monitorProcess != "") {
        try {
            ProcessClose(monitorProcess)
        } catch as e {
        }
    }
}

; === ENHANCED: Duplicate Detection with Detailed Logging ===
CheckForRealDuplicates(sourceFile) {
    if !FileExist(sourceFile)
        return false
        
    ; Create detailed log file
    logFile := SCANNING_LOGS_DIR . "\duplicate_detection_log.txt"
    FileAppend("=== DUPLICATE DETECTION LOG - " . FormatTime(, "yyyy-MM-dd HH:mm:ss") . " ===`n", logFile)
    FileAppend("Source file: " . sourceFile . "`n`n", logFile)
        
    fileContent := SafeFileRead(sourceFile)
    lines := StrSplit(fileContent, "`n")
    titleMap := Map()  ; title -> array of [sku, itemNumber, fullLine]
    
    FileAppend("Total lines to process: " . lines.Length . "`n`n", logFile)
    
    lineCount := 0
    processedCount := 0
    skipCount := 0
    
    for line in lines {
        lineCount++
        
        if (line = "" || Trim(line) = "") {
            FileAppend("Line " . lineCount . ": SKIPPED (empty)`n", logFile)
            skipCount++
            continue
        }
            
        ; Log the raw line first
        FileAppend("--- Line " . lineCount . " ---`n", logFile)
        FileAppend("Raw line: [" . line . "]`n", logFile)
        FileAppend("Line length: " . StrLen(line) . "`n", logFile)
        
        ; Extract title, SKU, and item number from line
        ; Format: "Title - SKU: ABC123 - Location - Item: 123456789"
        titleEnd := InStr(line, " - SKU: ") - 1
        if (titleEnd <= 0) {
            FileAppend("ERROR: No ' - SKU: ' found in line`n", logFile)
            FileAppend("InStr result: " . InStr(line, " - SKU: ") . "`n`n", logFile)
            skipCount++
            continue
        }
        
        title := SubStr(line, 1, titleEnd)
        FileAppend("Extracted title: [" . title . "]`n", logFile)
        
        ; Extract SKU - Enhanced parsing with better error checking
        skuStart := InStr(line, " - SKU: ") + 8
        skuSearchStart := skuStart
        skuEnd := InStr(line, " - ", , skuSearchStart) - 1
        
        if (skuEnd <= skuStart) {
            FileAppend("ERROR: Could not find SKU end marker`n", logFile)
            FileAppend("skuStart: " . skuStart . ", skuEnd: " . skuEnd . "`n", logFile)
            FileAppend("Searching from position: " . skuSearchStart . "`n", logFile)
            FileAppend("Looking for ' - ' after position " . skuSearchStart . "`n`n", logFile)
            skipCount++
            continue
        }
        
        sku := SubStr(line, skuStart, skuEnd - skuStart + 1)
        FileAppend("Extracted SKU: [" . sku . "]`n", logFile)
        
        ; Extract item number
        itemStart := InStr(line, " - Item: ") + 9
        if (itemStart <= 8) {
            FileAppend("ERROR: No ' - Item: ' found in line`n", logFile)
            FileAppend("InStr result for ' - Item: ': " . InStr(line, " - Item: ") . "`n`n", logFile)
            skipCount++
            continue
        }
        
        itemNumber := SubStr(line, itemStart)
        ; Clean up item number (remove any trailing whitespace/newlines)
        itemNumber := Trim(itemNumber, " `t`n`r")
        FileAppend("Extracted item number: [" . itemNumber . "]`n", logFile)
        FileAppend("Item number length: " . StrLen(itemNumber) . "`n", logFile)
        
        ; Validate item number format (should be 12 digits)
        if (!RegExMatch(itemNumber, "^\d{12}$")) {
            FileAppend("WARNING: Item number doesn't match expected 12-digit format`n", logFile)
            FileAppend("Item number regex test: " . (RegExMatch(itemNumber, "^\d+$") ? "digits only" : "contains non-digits") . "`n", logFile)
        }
        
        processedCount++
        
        ; Check for duplicates
        if !titleMap.Has(title) {
            titleMap[title] := [[sku, itemNumber, line]]
            FileAppend("FIRST occurrence of title - added to map`n`n", logFile)
        } else {
            FileAppend("DUPLICATE TITLE DETECTED - checking for real duplicate...`n", logFile)
            
            ; Check if this is a real duplicate or false positive
            isRealDuplicate := false
            duplicateReason := ""
            existingEntries := titleMap[title]
            
            FileAppend("Comparing against " . existingEntries.Length . " existing entries:`n", logFile)
            
            entryNum := 0
            for entry in existingEntries {
                entryNum++
                existingSku := entry[1]
                existingItemNumber := entry[2]
                existingLine := entry[3]
                
                FileAppend("  Entry " . entryNum . ":`n", logFile)
                FileAppend("    Existing SKU: [" . existingSku . "]`n", logFile)
                FileAppend("    Existing Item: [" . existingItemNumber . "]`n", logFile)
                FileAppend("    Current SKU: [" . sku . "]`n", logFile)
                FileAppend("    Current Item: [" . itemNumber . "]`n", logFile)
                
                ; Compare SKUs
                skuMatch := (existingSku = sku)
                FileAppend("    SKU match: " . (skuMatch ? "YES" : "NO") . "`n", logFile)
                
                ; Compare item numbers
                itemMatch := (existingItemNumber = itemNumber)
                FileAppend("    Item number match: " . (itemMatch ? "YES" : "NO") . "`n", logFile)
                
                if (skuMatch && !itemMatch) {
                    ; Same title and SKU but different item numbers = real duplicate
                    isRealDuplicate := true
                    duplicateReason := "Same title + Same SKU + Different item numbers"
                    FileAppend("    REAL DUPLICATE DETECTED: " . duplicateReason . "`n", logFile)
                    break
                } else if (skuMatch && itemMatch) {
                    ; Same title, SKU, and item number = false positive (scanned twice)
                    FileAppend("    FALSE POSITIVE: Same title + Same SKU + Same item number (likely scanned twice)`n", logFile)
                    continue
                } else if (!skuMatch) {
                    FileAppend("    Different SKUs - this is acceptable (same title, different products)`n", logFile)
                }
            }
            
            if (isRealDuplicate) {
                FileAppend("FINAL RESULT: REAL DUPLICATE - " . duplicateReason . "`n", logFile)
                FileAppend("  Current: Item " . itemNumber . " - " . line . "`n", logFile)
                for entry in existingEntries {
                    if (entry[1] = sku) {  ; Show matching SKU entry
                        FileAppend("  Conflicting: Item " . entry[2] . " - " . entry[3] . "`n", logFile)
                    }
                }
                FileAppend("`n", logFile)
                return true
            } else {
                FileAppend("FINAL RESULT: Not a real duplicate`n`n", logFile)
            }
            
            titleMap[title].Push([sku, itemNumber, line])
        }
    }
    
    ; Summary
    FileAppend("=== SUMMARY ===`n", logFile)
    FileAppend("Total lines: " . lineCount . "`n", logFile)
    FileAppend("Processed: " . processedCount . "`n", logFile)
    FileAppend("Skipped: " . skipCount . "`n", logFile)
    FileAppend("Unique titles: " . titleMap.Count . "`n", logFile)
    FileAppend("Real duplicates found: NO`n", logFile)
    FileAppend("=== END LOG ===`n`n", logFile)
    
    return false
}

; === NEW: Enhanced Duplicate Detection with Item Number Checking ===
UpdateStatistics(listingType) {
    global prefixStats, totalScannedToday
    
    ; Read the combined file and update statistics
    sourceFile := OUTPUT_FOLDER_DIR . "\all_item_numbers_" . listingType . ".txt"
    if !FileExist(sourceFile)
        return
        
    fileContent := SafeFileRead(sourceFile)
    lines := StrSplit(fileContent, "`n")
    
    for line in lines {
        if (line = "")
            continue
            
        ; Extract SKU prefix (first 2-3 characters before the dash)
        skuStart := InStr(line, " - SKU: ")
        if (skuStart > 0) {
            skuContent := SubStr(line, skuStart + 8)
            ; Find the end of the SKU (next " - ")
            skuEnd := InStr(skuContent, " - ")
            if (skuEnd > 0) {
                sku := SubStr(skuContent, 1, skuEnd - 1)
                if (RegExMatch(sku, "^([A-Z]{2,3})", &match)) {
                    prefix := match[1]
                    
                    ; Update total scanned count for this prefix
                    if (!prefixStats.Has(prefix))
                        prefixStats[prefix] := 0
                    prefixStats[prefix]++
                    totalScannedToday++
                }
            }
        }
    }
    
    ; Check for issues and update issue statistics
    CheckAndUpdateIssueStats(listingType)
}

CheckAndUpdateIssueStats(listingType) {
    global prefixIssues, totalIssuesFound
    
    ; Check empty SKUs
    emptyFile := OUTPUT_FOLDER_DIR . "\empty_skus_" . listingType . ".txt"
    if FileExist(emptyFile) {
        content := SafeFileRead(emptyFile)
        if (content != "") {
            lines := StrSplit(content, "`n")
            for line in lines {
                if (line = "")
                    continue
                    
                if (RegExMatch(line, "SKU:\s*([A-Z]{2,3})", &match)) {
                    prefix := match[1]
                    if (!prefixIssues.Has(prefix))
                        prefixIssues[prefix] := 0
                    prefixIssues[prefix]++
                    totalIssuesFound++
                }
            }
        }
    }
    
    ; Check duplicate titles
    duplicateFile := OUTPUT_FOLDER_DIR . "\duplicate_titles_" . listingType . ".txt"
    if FileExist(duplicateFile) {
        content := SafeFileRead(duplicateFile)
        if (content != "") {
            ; Count duplicate entries
            duplicateCount := 0
            lines := StrSplit(content, "`n")
            for line in lines {
                if InStr(line, "Duplicate Title:") || InStr(line, "Duplicate SKU Number") {
                    duplicateCount++
                }
                ; Extract prefix from duplicate entries
                if (RegExMatch(line, "SKU:\s*([A-Z]{2,3})", &match)) {
                    prefix := match[1]
                    if (!prefixIssues.Has(prefix))
                        prefixIssues[prefix] := 0
                    prefixIssues[prefix]++
                    totalIssuesFound++
                }
            }
        }
    }
}

GenerateReportMessage(isFullReport := false) {
    global scriptStartTime, prefixStats, prefixIssues, totalScannedToday, totalIssuesFound
    
    currentTime := FormatTime(, "yyyy-MM-dd HH:mm:ss")
    
    ; Calculate runtime
    runtime := (A_TickCount - scriptStartTime) / 1000 / 3600  ; Convert to hours
    runtimeStr := Format("{:.1f}", runtime) . " hours"
    
    ; Build report message
    reportMsg := ""
    
    if (isFullReport) {
        reportMsg .= "📊 **Daily eBay Scanning Report** - " . FormatTime(, "yyyy-MM-dd") . "`n`n"
    } else {
        reportMsg .= "📈 **Hourly Mini Report** - " . FormatTime(, "HH:mm") . "`n`n"
    }
    
    reportMsg .= "⏱️ **Runtime:** " . runtimeStr . "`n"
    reportMsg .= "📋 **Total Scanned:** " . totalScannedToday . " listings`n"
    reportMsg .= "⚠️ **Issues Found:** " . totalIssuesFound . " items`n`n"
    
    ; Top prefixes by scan count
    if (prefixStats.Count > 0) {
        reportMsg .= "🔝 **Most Scanned Initials:**`n"
        sortedPrefixes := SortMapByValue(prefixStats, true)  ; true for descending
        count := 0
        for prefix, scanCount in sortedPrefixes {
            if (++count > 5)  ; Show top 5
                break
            reportMsg .= "   • " . prefix . ": " . scanCount . " listings`n"
        }
        reportMsg .= "`n"
    }
    
    ; Top prefixes by issues
    if (prefixIssues.Count > 0) {
        reportMsg .= "⚠️ **Most Issues by Initials:**`n"
        sortedIssues := SortMapByValue(prefixIssues, true)  ; true for descending
        count := 0
        for prefix, issueCount in sortedIssues {
            if (++count > 5)  ; Show top 5
                break
            reportMsg .= "   • " . prefix . ": " . issueCount . " issues`n"
        }
        reportMsg .= "`n"
    }
    
    if (isFullReport) {
        ; No footer; scanning ends at 3:30 PM EST
    } else {
        ; No mini report hint
    }
    
    return reportMsg
}

SortMapByValue(mapToSort, descending := false) {
    ; Convert Map to array of [key, value] pairs
    pairs := []
    for key, value in mapToSort {
        pairs.Push([key, value])
    }
    
    ; Sort the array by value
    sortedPairs := []
    while (pairs.Length > 0) {
        maxIndex := 1
        for i, pair in pairs {
            if (descending) {
                if (pair[2] > pairs[maxIndex][2])
                    maxIndex := i
            } else {
                if (pair[2] < pairs[maxIndex][2])
                    maxIndex := i
            }
        }
        sortedPairs.Push(pairs[maxIndex])
        pairs.RemoveAt(maxIndex)
    }
    
    ; Convert back to Map
    sortedMap := Map()
    for pair in sortedPairs {
        sortedMap[pair[1]] := pair[2]
    }
    
    return sortedMap
}

SendReport(message) {
    ; Write message to temp file
    tempFile := A_Temp . "\scan_report.txt"
    FileAppend(message, tempFile)
    
    ; Send using Python script with positional message only (no flags)
    try {
        RunWait('python "' . A_ScriptDir . '\testmattermostmsg.py" "' . message . '"', , "Hide")
    } catch Error as err {
        ; Log error but don't stop script
        debugFile := SCANNING_LOGS_DIR . "\scan_report_errors.log"
        FileAppend(FormatTime(, "yyyy-MM-dd HH:mm:ss") . " - Error sending report: " . err.Message . "`n", debugFile)
    }
    
    ; Clean up temp file
    if FileExist(tempFile)
        FileDelete(tempFile)
}

CheckReportingTime() {
    global lastMiniReportTime
    
    currentHour := FormatTime(, "HH")
    currentMinute := FormatTime(, "mm")
    
    
    ; Hourly mini reports have been disabled – no further automatic reports
    return false
}

; === Helper Functions for Human-like Behavior ===
RandomSleep(min, max) {
    Sleep Random(min, max)
}

; === Prerequisite check (Python deps, AI modules) ===
RunPrereqCheck() {
    try {
        ; User preference: disable popup message box for prereq check (run silently)
        RunWait('python "' . A_ScriptDir . '\tools\prereq_check.py"', , "Hide")
    } catch as e {
        ; Ignore if Python not available or any error occurs
    }
}

HumanWaitForPage() {
    ; Simulate human waiting for page load (2-4 seconds)
    RandomSleep(2000, 7000)
}

HumanActionDelay() {
    ; Small delay between actions (200-800ms)
    RandomSleep(200, 800)
}

TabSwitchDelay() {
    ; Delay when switching tabs (600-1200ms)
    RandomSleep(600, 1200)
}

CloseTabDelay() {
    ; Delay after closing tab (800-1500ms)
    RandomSleep(800, 1500)
}

; === Cache Clearing Function ===
ClearChromeCache() {
    ; USER INTENT: NEVER terminate Chrome. Avoid triggering "Restore tabs" prompt.
    ; If desired, best-effort cache clear without killing the process.
    ; Safe no-op when flags are unsupported; ignore errors.
    try {
        Run "chrome.exe --no-startup-window --clear-data=5 --clear-data-begin=0 --clear-data-end=0"
    } catch as _ {
    }
    RandomSleep(1000, 1500)
    return true
}

EnsureIgnoreFiles() {
    if !DirExist(STATE_DIR)
        DirCreate STATE_DIR
    files := [STATE_DIR . "\\_ignore_list_scheduled.txt", STATE_DIR . "\\_ignore_list_active.txt"]
    for file in files {
        if !FileExist(file)
            FileAppend("", file)  ; Create empty file
    }
}

; === Enhanced Window Focus Function ===
EnsureChromeActive() {
    maxAttempts := 3
    currentAttempt := 0
    
    while (currentAttempt < maxAttempts) {
        if WinActive("ahk_exe chrome.exe") {
            return true
        }
        
        ; Try to activate the window
        if WinExist("ahk_exe chrome.exe") {
            WinActivate("ahk_exe chrome.exe")
            WinWaitActive("ahk_exe chrome.exe", , 2)  ; Wait up to 2 seconds
            
            if WinActive("ahk_exe chrome.exe") {
                RandomSleep(100, 300)  ; Small delay after activation
                return true
            }
        } else {
            return false  ; Chrome not running
        }
        
        currentAttempt++
        RandomSleep(500, 1000)  ; Wait before retry
    }
    
    return false
}

; === Core Functions ===
OpenURLAndGetContent(url) {
    global isFirstTab
    global isFirstActiveTab
    
    ; Determine if we should close this tab after reading content
    closeAfterRead := !isFirstTab
    
    if (isFirstTab) {
        Run "chrome.exe " url
        isFirstTab := false
    } else if (InStr(url, "/active?")) {
        if (isFirstActiveTab) {
            Run "chrome.exe " url
            HumanActionDelay()
            isFirstActiveTab := false
        } else {
            Run "chrome.exe " url
            HumanActionDelay()
        }
    } else {
        Run "chrome.exe " url
        HumanActionDelay()
    }
    
    WinWait("ahk_exe chrome.exe")
    if !EnsureChromeActive()
        return false
    
    maxAttempts := 2
    currentAttempt := 0
    lastContent := ""
    
    while (currentAttempt < maxAttempts) {
        startTime := A_TickCount
        maxWaitTime := 30000
        resultsFound := false
        
        while (A_TickCount - startTime < maxWaitTime) {
            ; Ensure Chrome is active before every clipboard operation
            if !EnsureChromeActive() {
                RandomSleep(1000, 2000)
                continue
            }
            
            A_Clipboard := ""
            HumanActionDelay()
            
            ; Double-check focus right before copy operation
            if !WinActive("ahk_exe chrome.exe") {
                if !EnsureChromeActive()
                    continue
            }
            
            Send "^a^c"
            
            if !ClipWait(1)
                continue
                
            currentContent := A_Clipboard
            
            if InStr(currentContent, "Results:") && InStr(currentContent, "Edit. Listing") {
                if (currentContent = lastContent) {
                    HumanWaitForPage()  ; Wait extra time for stability
                    ; Close the current tab after we have stable content, except for the very first tab (buffer)
                    if (closeAfterRead) {
                        if EnsureChromeActive() {
                            Send "{Ctrl down}w{Ctrl up}"
                            CloseTabDelay()
                        }
                    }
                    return true
                }
                lastContent := currentContent
            }
            
            RandomSleep(300, 700)  ; Variable check interval
        }
        
        if !resultsFound {
            if (currentAttempt < maxAttempts - 1) {
                if !EnsureChromeActive()
                    return false
                    
                Send "{F5}"
                RandomSleep(1500, 2500)  ; Longer wait after refresh
            }
            currentAttempt++
        }
    }
    
    return false
}

IsLastPage(content) {
    ; Ensure content parameter is valid
    if (!IsSet(content) || content = "") {
        return false
    }

    ; Detects last page by matching end number with total
    resultsPattern := "Results:\s*(\d+(?:,\d+)?)-(\d+(?:,\d+)?)\s+of\s+(\d+(?:,\d+)?)"
    if (RegExMatch(content, resultsPattern, &match)) {
        try {
            endNumber := RegExReplace(match[2], ",", "")
            totalNumber := RegExReplace(match[3], ",", "")
            return Integer(endNumber) >= Integer(totalNumber)
        } catch {
            return false
        }
    }
    return false
}

ScanFromNotepad(pageNumber, listingType) {
    debugFile := OUTPUT_FOLDER_DIR . "\titles_" . listingType . "_page_" . pageNumber . ".txt"
    itemNumberFile := OUTPUT_FOLDER_DIR . "\item_numbers_" . listingType . "_page_" . pageNumber . ".txt"
    
    if !DirExist(OUTPUT_FOLDER_DIR)
        DirCreate OUTPUT_FOLDER_DIR
    
    if FileExist(debugFile)
        FileDelete(debugFile)
    if FileExist(itemNumberFile)
        FileDelete(itemNumberFile)
        
    FileAppend("", debugFile)
    FileAppend("", itemNumberFile)
    
    chunks := StrSplit(A_Clipboard, "EditLink. Edit. Listing ")
    debugOutput := ""
    itemNumberOutput := ""
    
    for i, chunk in chunks {
        if (i = 1)  ; Skip first chunk
            continue
            
        title := Trim(StrSplit(chunk, "`n", "`r")[1])
        skuPattern := "m)^([A-Z]{2})\s*-+\s*(\d*)\s*-*\s*(.+)$"
        itemNumberPattern := "\d{12}"
        
        for _, line in StrSplit(chunk, "`n", "`r") {
            if (RegExMatch(line, skuPattern, &match)) {
                sku := match[2] != "" 
                    ? Trim(match[1] . " - " . match[2] . " - " . match[3])
                    : Trim(match[1] . " - " . match[3])
                debugOutput .= title . " - SKU: " . sku . "`n"
                
                for _, numLine in StrSplit(chunk, "`n", "`r") {
                    if (RegExMatch(numLine, itemNumberPattern, &itemMatch)) {
                        itemNumberOutput .= title . " - SKU: " . sku . " - Item: " . itemMatch[0] . "`n"
                        break
                    }
                }
                break
            }
        }
    }
    
    FileAppend(debugOutput, debugFile)
    FileAppend(itemNumberOutput, itemNumberFile)
}

CombineFilesAndDelete(listingType) {
    combinedFile := OUTPUT_FOLDER_DIR . "\all_titles_" . listingType . ".txt"
    FileAppend("", combinedFile)
    
    combinedItemFile := OUTPUT_FOLDER_DIR . "\all_item_numbers_" . listingType . ".txt"
    FileAppend("", combinedItemFile)
    
    Loop Files, OUTPUT_FOLDER_DIR . "\titles_" . listingType . "_page_*.txt" {
        try {
            content := SafeFileRead(A_LoopFileFullPath)
            if (content != "")
                FileAppend(content . "`n", combinedFile)
        } catch as _ {
        }
        SafeDelete(A_LoopFileFullPath)
    }
    
    Loop Files, OUTPUT_FOLDER_DIR . "\item_numbers_" . listingType . "_page_*.txt" {
        try {
            content2 := SafeFileRead(A_LoopFileFullPath)
            if (content2 != "")
                FileAppend(content2 . "`n", combinedItemFile)
        } catch as _ {
        }
        SafeDelete(A_LoopFileFullPath)
    }
}

; Original SKU handling functions
HasValidDigitSequenceOrig(sku) => RegExMatch(sku, "\d{3,6}")
isSKUEmptyOrig(sku) => RegExMatch(sku, "^[A-Z]+ -+ [A-Z0-9]+$")

; New SKU handling functions that match Python implementation
HasValidDigitSequence(sku) {
    if (USE_STANDARDIZED_SKU_HANDLING) {
        ; Extract parts to match sku_utils.py logic
        parts := StrSplit(RegExReplace(sku, "[\s-]+", " "), " ")
        initials := "XX"
        numericId := ""
        
        ; Find initials
        if (parts.Length > 0 && RegExMatch(parts[1], "^[A-Z]{2,3}$"))
            initials := parts[1]
            
        ; Look for 3-6 digit number right after initials
        if (parts.Length > 1) {
            i := 2
            Loop parts.Length - 1 {
                if (RegExMatch(parts[i], "^\d{3,6}$")) {
                    numericId := parts[i]
                    break
                }
                i++
            }
        }
        
        ; Fallback: look for any 3-6 digit number anywhere
        if (numericId = "") {
            i := 1
            Loop parts.Length {
                if (RegExMatch(parts[i], "^\d{3,6}$")) {
                    numericId := parts[i]
                    break
                }
                i++
            }
        }
        
        ; Final fallback: any digit sequence
        if (numericId = "") {
            i := parts.Length
            Loop parts.Length {
                if (RegExMatch(parts[i], "^\d+$")) {
                    numericId := parts[i]
                    break
                }
                i--
            }
        }
        
        return numericId != ""
    } else {
        ; Original function
        return HasValidDigitSequenceOrig(sku)
    }
}

isSKUEmpty(sku) {
    if (USE_STANDARDIZED_SKU_HANDLING) {
        ; Match Python implementation
        if (RegExMatch(sku, "^[A-Z]{2,3}\s*-+\s*[A-Z0-9]+$") && !RegExMatch(sku, "\d{3,6}"))
            return true
        return false
    } else {
        ; Original function
        return isSKUEmptyOrig(sku)
    }
}

LoadIgnoreList(listingType) {
    ignoreMap := Map()
    ignoreFile := STATE_DIR . "\\_ignore_list_" . listingType . ".txt"
    
    if FileExist(ignoreFile) {
        Loop Parse, FileRead(ignoreFile), "`n" {
            if (A_LoopField != "")
                ignoreMap[Trim(RegExReplace(A_LoopField, "[\s\r\n]+", " "))] := true
        }
    }
    return ignoreMap
}

; === ENHANCED: ProcessSKUs with Detailed Logging and Item Number Checking ===
ProcessSKUs(listingType) {
    sourceFile := OUTPUT_FOLDER_DIR . "\all_item_numbers_" . listingType . ".txt"
    
    ; Create processing log
    processLogFile := SCANNING_LOGS_DIR . "\sku_processing_log_" . listingType . ".txt"
    FileAppend("=== SKU PROCESSING LOG - " . listingType . " - " . FormatTime(, "yyyy-MM-dd HH:mm:ss") . " ===`n`n", processLogFile)
    
    fileContent := SafeFileRead(sourceFile)
    lines := StrSplit(fileContent, "`n")
    ignoreList := LoadIgnoreList(listingType)

    emptySKUs := []
    nonEmptySKUs := []
    titleMap := Map()  ; title -> array of [sku, itemNumber, fullLine]
    skuMap := Map()    ; digitSKU -> array of [title, itemNumber, fullLine]
    
    FileAppend("Total lines to process: " . lines.Length . "`n", processLogFile)
    FileAppend("Ignore list entries: " . ignoreList.Count . "`n`n", processLogFile)
    
    lineNum := 0
    for line in lines {
        lineNum++
        
        if (line = "" || Trim(line) = "") {
            FileAppend("Line " . lineNum . ": SKIPPED (empty)`n", processLogFile)
            continue
        }
            
        normalizedLine := Trim(RegExReplace(line, "[\s\r\n]+", " "))
        if (ignoreList.Has(normalizedLine)) {
            FileAppend("Line " . lineNum . ": SKIPPED (in ignore list)`n", processLogFile)
            continue
        }
        
        ; Extract title, SKU, and item number from line
        ; Format: "Title - SKU: ABC123 - Location - Item: 123456789"
        titleEnd := InStr(line, " - SKU: ") - 1
        if (titleEnd <= 0) {
            FileAppend("Line " . lineNum . ": ERROR - No ' - SKU: ' found`n", processLogFile)
            continue
        }
        
        title := SubStr(line, 1, titleEnd)
        
        ; Extract SKU
        skuStart := InStr(line, " - SKU: ") + 8
        skuEnd := InStr(line, " - ", , skuStart) - 1
        if (skuEnd <= skuStart) {
            FileAppend("Line " . lineNum . ": ERROR - Could not find SKU end marker`n", processLogFile)
            continue
        }
        
        sku := SubStr(line, skuStart, skuEnd - skuStart + 1)
        
        ; Extract item number
        itemStart := InStr(line, " - Item: ") + 9
        if (itemStart <= 8) {
            FileAppend("Line " . lineNum . ": ERROR - No ' - Item: ' found`n", processLogFile)
            continue
        }
        
        itemNumber := Trim(SubStr(line, itemStart), " `t`n`r")

        FileAppend("Line " . lineNum . ": [" . title . "] SKU: [" . sku . "] Item: [" . itemNumber . "]`n", processLogFile)

        ; Check for digit sequences in SKU with item number verification
        if (RegExMatch(sku, "(\d{3,6})", &match)) {
            digitSKU := match[1]
            FileAppend("  Digit sequence found: " . digitSKU . "`n", processLogFile)
            
            if (!skuMap.Has(digitSKU)) {
                skuMap[digitSKU] := [[title, itemNumber, line]]
                FileAppend("  First occurrence of digit sequence`n", processLogFile)
            } else {
                ; Check if this is a real duplicate (different item numbers) or false positive (same item number)
                isRealDuplicate := false
                for entry in skuMap[digitSKU] {
                    existingItemNumber := entry[2]
                    if (existingItemNumber != itemNumber) {
                        isRealDuplicate := true
                        FileAppend("  REAL SKU DUPLICATE detected! Different item numbers: " . existingItemNumber . " vs " . itemNumber . "`n", processLogFile)
                        break
                    } else {
                        FileAppend("  FALSE POSITIVE: Same digit sequence + Same item number (likely scanned twice)`n", processLogFile)
                    }
                }
                
                if (isRealDuplicate) {
                    skuMap[digitSKU].Push([title, itemNumber, line])
                }
            }
        } else {
            FileAppend("  No 3-6 digit sequence found in SKU`n", processLogFile)
        }

        ; Check for title duplicates with item number verification
        if !titleMap.Has(title) {
            titleMap[title] := [[sku, itemNumber, line]]
            FileAppend("  First occurrence of title`n", processLogFile)
        } else {
            ; Check if this is a real duplicate or false positive
            isRealDuplicate := false
            for entry in titleMap[title] {
                existingItemNumber := entry[2]
                if (existingItemNumber != itemNumber) {
                    isRealDuplicate := true
                    FileAppend("  REAL TITLE DUPLICATE detected! Different item numbers: " . existingItemNumber . " vs " . itemNumber . "`n", processLogFile)
                    break
                } else {
                    FileAppend("  FALSE POSITIVE: Same title + Same item number (likely scanned twice)`n", processLogFile)
                }
            }
            
            if (isRealDuplicate) {
                titleMap[title].Push([sku, itemNumber, line])
            }
        }

        ; Check SKU validity
        if (!HasValidDigitSequence(sku)) {
            if (isSKUEmpty(sku)) {
                emptySKUs.Push(line)
                FileAppend("  SKU flagged as EMPTY`n", processLogFile)
            } else {
                nonEmptySKUs.Push(line)
                FileAppend("  SKU flagged as non-empty but invalid`n", processLogFile)
            }
        } else {
            FileAppend("  SKU is valid`n", processLogFile)
        }
    }
    
    ; Log duplicate summary
    FileAppend("`n=== DUPLICATE SUMMARY ===`n", processLogFile)
    
    realTitleDuplicates := 0
    realSKUDuplicates := 0
    
    for title, entries in titleMap {
        if (entries.Length > 1) {
            realTitleDuplicates++
            FileAppend("Real title duplicate: '" . title . "' has " . entries.Length . " different item numbers`n", processLogFile)
            for entry in entries {
                FileAppend("  Item: " . entry[2] . " - " . entry[3] . "`n", processLogFile)
            }
        }
    }
    
    for digitSKU, entries in skuMap {
        if (entries.Length > 1) {
            realSKUDuplicates++
            FileAppend("Real SKU duplicate: '" . digitSKU . "' has " . entries.Length . " different item numbers`n", processLogFile)
            for entry in entries {
                FileAppend("  Item: " . entry[2] . " - " . entry[3] . "`n", processLogFile)
            }
        }
    }
    
    FileAppend("Real title duplicates found: " . (realTitleDuplicates > 0 ? "YES (" . realTitleDuplicates . ")" : "NO") . "`n", processLogFile)
    FileAppend("Real SKU digit duplicates found: " . (realSKUDuplicates > 0 ? "YES (" . realSKUDuplicates . ")" : "NO") . "`n", processLogFile)

    FileAppend(JoinArray(emptySKUs), OUTPUT_FOLDER_DIR . "\empty_skus_" . listingType . ".txt")
    FileAppend(JoinArray(nonEmptySKUs), OUTPUT_FOLDER_DIR . "\non_empty_skus_" . listingType . ".txt")

    duplicateOutput := ""
    for title, entries in titleMap {
        if (entries.Length > 1) {
            duplicateOutput .= "Duplicate Title: " . title . "`n"
            for entry in entries {
                duplicateOutput .= "  " . entry[3] . "`n"
            }
            duplicateOutput .= "`n"
        }
    }
    
    for digitSKU, entries in skuMap {
        if (entries.Length > 1) {
            duplicateOutput .= "Duplicate SKU Number (" . digitSKU . "):`n"
            for entry in entries {
                duplicateOutput .= "  " . entry[3] . "`n"
            }
            duplicateOutput .= "`n"
        }
    }
    
    FileAppend(duplicateOutput, OUTPUT_FOLDER_DIR . "\duplicate_titles_" . listingType . ".txt")

    ; Handle duplicates immediately (same as empty SKUs)
    if (duplicateOutput != "") {
        SoundPlay "*-1"
        
        ; Build message in identical format to duplicate titles
        if (listingType = "active") {
            listingLabel := "Active"
        } else {
            listingLabel := "Scheduled"
        }
        
        ; Split duplicateOutput into lines and clean up
        duplicateLines := StrSplit(Trim(duplicateOutput), "`n")
        cleanedDuplicates := []
        for line in duplicateLines {
            if (Trim(line) != "") {
                cleanedDuplicates.Push(Trim(line))
            }
        }
        
        message := listingLabel . " duplicate titles:`n" . JoinArray(cleanedDuplicates)
        
        ; Add eBay search URL (use first actual listing line for keyword)
        firstListingLine := ""
        for line in cleanedDuplicates {
            ; Find the first line that contains " - SKU: " (actual listing, not header)
            if (InStr(line, " - SKU: ") > 0) {
                firstListingLine := line
                break
            }
        }
        
        if (firstListingLine != "") {
            ; Extract title before " - SKU: " for search keyword
            titleEnd := InStr(firstListingLine, " - SKU: ") - 1
            if (titleEnd > 0) {
                keyword := SubStr(firstListingLine, 1, titleEnd)
                ; URL encode the keyword (basic encoding)
                keyword := StrReplace(keyword, " ", "+")
                keyword := StrReplace(keyword, "&", "%26")
                
                if (listingType = "active") {
                    message .= "`n`nView Active Listings: https://www.ebay.com/sh/lst/active?keyword=" . keyword . "&source=filterbar&action=search"
                } else {
                    message .= "`n`nView Scheduled Listings: https://www.ebay.com/sh/lst/scheduled?keyword=" . keyword . "&source=filterbar&action=search"
                }
            }
        }
        
        ; Send message via testmattermostmsg.py (positional only)
        try {
            RunWait('python "' . A_ScriptDir . '\testmattermostmsg.py" "' . message . '"', , "Hide")
        } catch Error as err {
        }
        
        ; Add to ignore list (identical to empty SKUs)
        ignoreFile := STATE_DIR . "\\_ignore_list_" . listingType . ".txt"
        for line in cleanedDuplicates {
            FileAppend(line . "`n", ignoreFile)
        }
        
        ; Clear the duplicate file (identical to empty SKUs)
        duplicateFile := OUTPUT_FOLDER_DIR . "\duplicate_titles_" . listingType . ".txt"
        FileAppend("", duplicateFile)  ; Clear the file
        
        ; Open file for manual review
        SetTimer () => Run(duplicateFile), -1000
    }

    if (emptySKUs.Length > 0) {
        SoundPlay "*-1"
        
        ; Build message in identical format to duplicate titles
        if (listingType = "active") {
            listingLabel := "Active"
        } else {
            listingLabel := "Scheduled"
        }
        
        message := listingLabel . " empty SKUs:`n" . JoinArray(emptySKUs)
        
        ; Add eBay search URL (use first item for keyword)
        if (emptySKUs.Length > 0) {
            firstLine := emptySKUs[1]
            ; Extract title before " - SKU: " for search keyword
            titleEnd := InStr(firstLine, " - SKU: ") - 1
            if (titleEnd > 0) {
                keyword := SubStr(firstLine, 1, titleEnd)
                ; URL encode the keyword (basic encoding)
                keyword := StrReplace(keyword, " ", "+")
                keyword := StrReplace(keyword, "&", "%26")
                
                if (listingType = "active") {
                    message .= "`n`nView Active Listings: https://www.ebay.com/sh/lst/active?keyword=" . keyword . "&source=filterbar&action=search"
                } else {
                    message .= "`n`nView Scheduled Listings: https://www.ebay.com/sh/lst/scheduled?keyword=" . keyword . "&source=filterbar&action=search"
                }
            }
        }
        
        ; Send message via testmattermostmsg.py (positional only)
        try {
            RunWait('python "' . A_ScriptDir . '\testmattermostmsg.py" "' . message . '"', , "Hide")
        } catch Error as err {
        }
        
        ; Add to ignore list (identical to duplicates)
        ignoreFile := STATE_DIR . "\\_ignore_list_" . listingType . ".txt"
        for line in emptySKUs {
            FileAppend(line . "`n", ignoreFile)
        }
        
        ; Clear the empty SKU file (identical to duplicates)
        emptyFile := OUTPUT_FOLDER_DIR . "\empty_skus_" . listingType . ".txt"
        FileAppend("", emptyFile)  ; Clear the file
        
        ; Still open the file for manual review
        SetTimer () => Run(emptyFile), -1000
    }
}

CleanupFiles() {
    static files := [
        "empty_skus_scheduled.txt", "empty_skus_active.txt",
        "non_empty_skus_scheduled.txt", "non_empty_skus_active.txt",
        "duplicate_titles_scheduled.txt", "duplicate_titles_active.txt",
        "all_titles_scheduled.txt", "all_titles_active.txt",
        "all_item_numbers_scheduled.txt", "all_item_numbers_active.txt"
    ]
    
    for file in files {
        fullPath := OUTPUT_FOLDER_DIR . "\" . file
        if FileExist(fullPath)
            SafeDelete(fullPath)
    }
    
    Loop Files, OUTPUT_FOLDER_DIR . "\titles_scheduled_page_*.txt" {
        SafeDelete(A_LoopFileFullPath)
    }
    Loop Files, OUTPUT_FOLDER_DIR . "\titles_active_page_*.txt" {
        SafeDelete(A_LoopFileFullPath)
    }
    Loop Files, OUTPUT_FOLDER_DIR . "\item_numbers_scheduled_page_*.txt" {
        SafeDelete(A_LoopFileFullPath)
    }
    Loop Files, OUTPUT_FOLDER_DIR . "\item_numbers_active_page_*.txt" {
        SafeDelete(A_LoopFileFullPath)
    }
}

JoinArray(arr) {
    result := ""
    for item in arr
        result .= item . "`n"
    return Trim(result, "`n")
}

; === Resilient file helpers ===
SafeFileRead(path, retries := 3, delayMs := 150) {
    while (retries > 0) {
        try {
            return FileRead(path)
        } catch as _ {
            Sleep delayMs
            retries -= 1
        }
    }
    return ""
}

SafeDelete(path) {
    try {
        if FileExist(path)
            FileDelete(path)
    } catch as _ {
        ; ignore sharing violations
    }
}

CheckShutdownTime() {
    ; End-of-day cutoff at 3:30 PM EST without system shutdown
    if (FormatTime(, "HH") = "15" && FormatTime(, "mm") >= "30") {
        return true
    }
    return false
}

CloseAllNotepads() {
    while WinExist("ahk_exe notepad.exe") {
        WinClose("ahk_exe notepad.exe")
        if WinExist("ahk_exe notepad.exe")
            ProcessClose "notepad.exe"
    }
}

; === Initial Cleanup ===
EnsureLogDirectories()
EnsureStateDirectory()
MigrateLegacyState()
RunPrereqCheck()
CleanupFiles()
EnsureIgnoreFiles()

; === Initialize Reporting ===
InitializeReporting() {
    global prefixStats, prefixIssues, totalScannedToday, totalIssuesFound
    
    ; Initialize prefix tracking maps
    prefixStats := Map()
    prefixIssues := Map()
    totalScannedToday := 0
    totalIssuesFound := 0
    
    ; Create reports directory if it doesn't exist
    reportsDir := "reports"
    if !DirExist(reportsDir)
        DirCreate(reportsDir)
}

InitializeReporting()

; === Main Loop ===
Loop {
    try {
        ; Start the monitoring process on first loop
        if (firstLoop) {
            if (!NO_MONITOR)
                StartScanMonitor()
            
            ; Clear Chrome's cache on first run to ensure fresh data
            ClearChromeCache()
        }
        
        CleanupFiles()
        RandomSleep(500, 1000)

        ; Scrape scheduled listings (all pages)
        baseURL := "https://www.ebay.com/sh/lst/scheduled?action=sort&sort=-scheduledStartDate&offset="
        offset := 0
        pageLimit := 50
        pageNumber := 1

        while (true) {
            url := baseURL . offset . "&limit=" . pageLimit
            if !OpenURLAndGetContent(url)
                continue
            
            HumanWaitForPage()
            clipContent := A_Clipboard
            if IsLastPage(clipContent) {
                ScanFromNotepad(pageNumber, "scheduled")
                RandomSleep(500, 1000)
                break
            }
            
            ScanFromNotepad(pageNumber, "scheduled")
            RandomSleep(800, 1500)
            pageNumber++
            offset += pageLimit
        }
        
        CombineFilesAndDelete("scheduled")
        UpdateStatistics("scheduled")  ; Update statistics
        RandomSleep(500, 1000)

        ; Run duplicate detection for logging/reporting only (no retry)
        CheckForRealDuplicates(OUTPUT_FOLDER_DIR . "\all_item_numbers_scheduled.txt")

        ; Scrape active listings
        baseActiveURL := "https://www.ebay.com/sh/lst/active?action=sort&sort=-scheduledStartDate&offset="
        activeOffset := 0
        activePageLimit := 50
        activePageNumber := 1
        activePagesProcessed := 0

        while (true) {
            url := baseActiveURL . activeOffset . "&limit=" . activePageLimit
            if !OpenURLAndGetContent(url)
                continue
            
            clipContent := A_Clipboard
            ScanFromNotepad(activePageNumber, "active")
            activePagesProcessed++
            
            if IsLastPage(clipContent) {
                break
            }
            
            maxActivePagesToProcess := firstLoop ? 1 : 1
            
            if (activePagesProcessed >= maxActivePagesToProcess) {
                break
            }
            
            activePageNumber++
            activeOffset += activePageLimit
            RandomSleep(400, 800)
        }
        
        CombineFilesAndDelete("active")
        UpdateStatistics("active")  ; Update statistics
        RandomSleep(500, 1000)

        ; Run duplicate detection for logging/reporting only (no retry)
        CheckForRealDuplicates(OUTPUT_FOLDER_DIR . "\all_item_numbers_active.txt")

        ProcessSKUs("scheduled")
        ProcessSKUs("active")

        firstLoop := false

        ; Reset Chrome and tab management state between loops
        if WinExist("ahk_exe chrome.exe") {
            if EnsureChromeActive() {
                Send "{Ctrl down}w{Ctrl up}"
                CloseTabDelay()
            }
        }
        isFirstTab := true
        isFirstActiveTab := true
        
        ; NEW: Check for reporting time
        CheckReportingTime()
        
        if (CheckShutdownTime()) {
            ; Pause zscrape at end-of-day but keep Scan Monitor running
            break
        }
            
        SetTimer(CloseAllNotepads, -1000)
        RunWait(A_ScriptDir . "\individual_listing_scrape_caller.ahk")
        RandomSleep(10000, 30000)

    } catch as e {
        try {
            errFile := MONITORING_LOGS_DIR . "\zscrape_runtime_errors.log"
            FileAppend(FormatTime(, "yyyy-MM-dd HH:mm:ss") . " - " . e.Message . "`n", errFile)
        } catch as _ {
        }
        Sleep 2000
        continue
    }
}