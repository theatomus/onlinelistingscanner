#Requires AutoHotkey v2.0
#SingleInstance force

; Accept itemNumber from command line, or enter TEST MODE when launched with no args
isTestMode := (A_Args.Length = 0)
if (isTestMode) {
    itemNumber := "test description"
} else {
    itemNumber := A_Args[1]
}

; Set working directory to script directory
SetWorkingDir A_ScriptDir
SendMode "Input"

; Define directories
specsDir := A_ScriptDir . "\specs_data"
if !DirExist(specsDir)
    DirCreate specsDir

; Create centralized logs directory structure
logsDir := A_ScriptDir . "\logs"
processingLogsDir := logsDir . "\processing"
logDir := processingLogsDir . "\pull_logs"
if !DirExist(logsDir)
    DirCreate logsDir
if !DirExist(processingLogsDir)
    DirCreate processingLogsDir
if !DirExist(logDir)
    DirCreate logDir
if !DirExist(logDir)
    DirCreate logDir

; Define log file with item number (standardized format)
logFile := logDir . "\" . itemNumber . "_pull_log.txt"

; Log message function
LogMessage(message) {
    FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - " . message . "`n", logFile
}

; === Heartbeat and restart signaling (used by Scan Monitor) ===
; Keeps a simple heartbeat file updated every 5s and allows signaling
; a restart request instead of hard erroring on window targeting failures.
stateDir := A_ScriptDir . "\state"
if !DirExist(stateDir)
    DirCreate stateDir
global heartbeatFile := stateDir . "\ahk_heartbeat.txt"
global restartSignalFile := stateDir . "\ahk_restart_request.txt"

TouchHeartbeat() {
    global heartbeatFile, itemNumber
    try {
        FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - scan_edit_specifics - " . itemNumber . "`n", heartbeatFile, "UTF-8"
    } catch as _ {
    }
}

SignalRestartAndExit(reason) {
    global restartSignalFile, itemNumber
    try {
        content := FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - " . reason . " - " . itemNumber . "`n"
        FileAppend content, restartSignalFile, "UTF-8"
    } catch as _ {
    }
    LogMessage("Requesting restart: " . reason)
    ExitApp
}

; Start periodic heartbeat
SetTimer TouchHeartbeat, 5000
TouchHeartbeat()

; Helper functions for human-like behavior
RandomSleep(min, max) {
    Sleep Random(min, max)
}

HumanWaitForPage() {
    CheckPause()
    RandomSleep(858, 1450)  ; Simulate page load delay
}

HumanActionDelay() {
    CheckPause()
    RandomSleep(200, 400)    ; Simulate human action delay
}

; CapsLock pause functionality
CheckPause() {
    while (GetKeyState("CapsLock", "T")) {
        Sleep 100
    }
}

; Function to get HTML from clipboard
GetClipboardHTML() {
    htmlContent := ""
    try {
        if DllCall("OpenClipboard", "ptr", 0) {
            htmlFormat := DllCall("RegisterClipboardFormat", "str", "HTML Format", "uint")
            if (htmlHandle := DllCall("GetClipboardData", "uint", htmlFormat, "ptr")) {
                if (htmlPtr := DllCall("GlobalLock", "ptr", htmlHandle, "ptr")) {
                    htmlSize := DllCall("GlobalSize", "ptr", htmlHandle, "uptr")
                    htmlContent := StrGet(htmlPtr, htmlSize, "UTF-8")
                    DllCall("GlobalUnlock", "ptr", htmlHandle)
                }
            }
            DllCall("CloseClipboard")
        }
    }
    return htmlContent
}

; Quickly TAB through focusable elements to find the Description editor and copy its full HTML
; Heuristic: when focused inside the description, Ctrl+A/C yields text containing
; either "Functional Condition" or "Disclaimer:" (case-insensitive).
CaptureDescriptionByTabScanning(itemNumber) {
    LogMessage("Attempting reverse TAB-scan (Shift+Tab) from bottom to isolate Description editor")
    found := false
    insideDescEditor := false

    ; Try to move focus near the bottom of the page
    if EnsureChromeActive() {
        Send "^{End}"
        Sleep 400
        screenWidth := A_ScreenWidth
        screenHeight := A_ScreenHeight
        ; Click near bottom-left to avoid 'Back to top' controls typically on right
        bottomClickX := screenWidth * 0.20
        bottomClickY := screenHeight * 0.88
        MouseClick "left", bottomClickX, bottomClickY
        Sleep 250
    }

    ; Primary: reverse scan upwards with Shift+Tab and perform Ctrl+A + Ctrl+C at each step
    maxBackTabs := 100
    Loop maxBackTabs {
        CheckPause()
        Send "+{Tab}"
        Sleep 50

        ; Always select-all and copy to detect when focus is inside the description editor
        A_Clipboard := ""
        Send "^a"
        Sleep 60
        Send "^c"
        if !ClipWait(0.3) {
            ; If we've attempted 80 Shift+Tab steps without reliable clipboard data, fallback to newreader
            if (A_Index >= 80) {
                LogMessage("Description not found after 80 Shift+Tab reads; invoking newreader fallback for non‑editable listing")
                try {
                    RunWait('"' . A_ScriptDir . '\\newreader.ahk" "' . itemNumber . '"')
                } catch Error as err {
                    LogMessage("ERROR launching newreader fallback: " . err.Message)
                }
                ; Consider success if newreader wrote the description HTML mirror
                descHtmlPath := A_ScriptDir . "\\item_contents\\" . itemNumber . "_description_html.txt"
                if FileExist(descHtmlPath) {
                    LogMessage("Fallback succeeded: description HTML found at " . descHtmlPath)
                    found := true
                } else {
                    LogMessage("Fallback did not produce description HTML; continuing")
                }
                break
            }
            continue
        }
        text := A_Clipboard
        rawHtml := GetClipboardHTML()
        html := NormalizeClipboardHTML(rawHtml)

        ; Do not rely on nearby control heuristics; only keyword-detect using plaintext

        ; Detect using plaintext only; once found, treat as being inside description editor
        keywordHit := RegExMatch(text, "i)(functional\s*condition|disclaimer:)")
        if (keywordHit && StrLen(text) > 20) {
            ; Ensure full capture
            A_Clipboard := ""
            Send "^a"
            Sleep 60
            Send "^c"
            ClipWait(0.3)
            text := A_Clipboard
            rawHtml := GetClipboardHTML()
            html := NormalizeClipboardHTML(rawHtml)
            ; First save mirrors so _description.txt exists
            SaveDescriptionFiles(itemNumber, html, text)
            ; Also save table data like newreader using ClipboardAll
            LogMessage("Attempting to extract table HTML data (clipboard all)")
            ExtractTableHTMLLikeNewreader(itemNumber)
            ; Verify table file was created
            try {
                tableDir := A_ScriptDir . "\table_data"
                tableFile := tableDir . "\" . itemNumber . "_table.raw"
                if FileExist(tableFile) {
                    fileSize := FileGetSize(tableFile)
                    LogMessage("Table raw file created successfully, size: " . fileSize . " bytes")
                } else {
                    LogMessage("WARNING: Table raw file was not created")
                }
            } catch as e {
                LogMessage("ERROR verifying table raw file: " . e.Message)
            }
            ; Then process like newreader to normalize description into _description.txt
            ProcessDescriptionContent(html, itemNumber)
            ; Also mirror into the base item text file section for visibility
            UpdateBaseDescriptionFile(itemNumber, text)
            found := true
            break
        }

        ; If not found by 80 Shift+Tab steps, skip remaining scan and fallback to newreader
        if (A_Index >= 80 && !found) {
            LogMessage("Description not found after 80 Shift+Tab reads; invoking newreader fallback for non‑editable listing")
            try {
                RunWait('"' . A_ScriptDir . '\\newreader.ahk" "' . itemNumber . '"')
            } catch Error as err {
                LogMessage("ERROR launching newreader fallback: " . err.Message)
            }
            descHtmlPath := A_ScriptDir . "\\item_contents\\" . itemNumber . "_description_html.txt"
            if FileExist(descHtmlPath) {
                LogMessage("Fallback succeeded: description HTML found at " . descHtmlPath)
                found := true
            } else {
                LogMessage("Fallback did not produce description HTML")
            }
            break
        }
    }

    ; Removed forward scan fallback per user feedback to avoid jumping to top

    if !found {
        LogMessage("TAB-scan could not locate Description editor (keywords not found)")
    }
    return found
}

SaveDescriptionFiles(itemNumber, htmlContent, textContent) {
    try {
        itemDir := A_ScriptDir . "\item_contents"
        if !DirExist(itemDir)
            DirCreate itemDir
        
        htmlFile := itemDir . "\" . itemNumber . "_description_html.txt"
        textFile := itemDir . "\" . itemNumber . "_description.txt"
        
        if FileExist(htmlFile)
            FileDelete htmlFile
        if (htmlContent != "")
            FileAppend htmlContent, htmlFile, "UTF-8"
        else
            FileAppend textContent, htmlFile, "UTF-8"
        LogMessage("Saved description HTML to " . htmlFile)
        
        ; Minimal text output that downstream parsers can read
        descOut := "=== ITEM DESCRIPTION ===`n" . textContent
        if FileExist(textFile)
            FileDelete textFile
        FileAppend descOut, textFile, "UTF-8"
        LogMessage("Saved description text to " . textFile)
    } catch as e {
        LogMessage("ERROR saving description files: " . e.Message)
    }
}

NormalizeClipboardHTML(rawHtml) {
    try {
        ; Strip CF_HTML header lines like Version/StartHTML/EndHTML/SourceURL
        cleaned := RegExReplace(rawHtml, "(?s)^Version:.*?\r?\n<html>", "<html>")
        cleaned := RegExReplace(cleaned, "(?i)<!--StartFragment-->", "")
        cleaned := RegExReplace(cleaned, "(?i)<!--EndFragment-->", "")
        return cleaned
    } catch as e {
        return rawHtml
    }
}

ExtractTextFromHTML(htmlContent) {
    cleanText := htmlContent
    cleanText := StrReplace(cleanText, "&amp;", "&")
    cleanText := StrReplace(cleanText, "&lt;", "<")
    cleanText := StrReplace(cleanText, "&gt;", ">")
    cleanText := StrReplace(cleanText, "&quot;", '"')
    cleanText := StrReplace(cleanText, "&#39;", "'")
    cleanText := StrReplace(cleanText, "&nbsp;", " ")
    cleanText := RegExReplace(cleanText, "i)</?p[^>]*>", "`n")
    cleanText := RegExReplace(cleanText, "i)<br[^>]*>", "`n")
    cleanText := RegExReplace(cleanText, "i)</div>", "`n")
    cleanText := RegExReplace(cleanText, "<[^>]*>", "")
    cleanText := RegExReplace(cleanText, "[ \t]+", " ")
    cleanText := RegExReplace(cleanText, "\n[ \t]*\n", "`n")
    cleanText := RegExReplace(cleanText, "^\s+|\s+$", "")
    return cleanText
}

CheckConditions(descText) {
    result := Map()
    result["hasError"] := false
    result["hasWarning"] := false
    result["messages"] := []

    disclaimerStartPos := InStr(descText, "Disclaimer:")
    if (disclaimerStartPos > 0) {
        result["cleanedDesc"] := Trim(SubStr(descText, 1, disclaimerStartPos - 1))
        if (result["cleanedDesc"] = "") {
            result["messages"].Push("WARNING: Text removed completely")
        }
    } else {
        result["messages"].Push("WARNING: Standard disclaimer text is missing or incorrect")
        result["cleanedDesc"] := descText
    }

    desc := Trim(result["cleanedDesc"])
    lines := StrSplit(desc, "`n", "`r")

    cosmeticFound := false
    for _, line in lines {
        if (RegExMatch(line, "i).*(C[2-6]|C8|C9|C10)\s*-\s*.*", &m) || RegExMatch(line, "i).*\bCosmetic\s*Condition\b.*", &m)) {
            cosmeticFound := true
            break
        }
    }
    if (!cosmeticFound) {
        result["messages"].Push("INFO: Cosmetic Condition not found")
    }

    functionalFound := false
    for _, line in lines {
        if (RegExMatch(line, "i).*\bFunctional\s*Condition\b.*", &m)) {
            functionalFound := true
            break
        }
    }
    if (!functionalFound) {
        if (RegExMatch(desc, "i)\bF[1-6]|F10\b[^\w]*\w+", &m)) {
            functionalFound := true
            result["messages"].Push("INFO: Functional Condition detected via condition code pattern")
        } else {
            result["messages"].Push("WARNING: Functional Condition not found - manual review required")
        }
    }

    dataSanitizationFound := false
    for _, line in lines {
        if (RegExMatch(line, "i).*(no\s*data|non[- ]*data|data\s*sanitization).*", &m)) {
            dataSanitizationFound := true
            break
        }
    }
    if (!dataSanitizationFound) {
        result["messages"].Push("INFO: Data Sanitization statement not found")
    }

    return result
}

TruncateTableContent(content) {
    tablePos := InStr(content, "Make`tModel`tCPU")
    if (tablePos > 0) {
        return Trim(SubStr(content, 1, tablePos - 1))
    }
    return content
}

ProcessDescriptionContent(htmlContent, itemNumber) {
    LogMessage("Processing description content for item: " . itemNumber)
    cleanText := ExtractTextFromHTML(htmlContent)
    conditionResults := CheckConditions(cleanText)
    description := TruncateTableContent(conditionResults["cleanedDesc"])

    itemDir := A_ScriptDir . "\item_contents"
    if !DirExist(itemDir)
        DirCreate itemDir
    descriptionFile := itemDir . "\" . itemNumber . "_description.txt"

    if FileExist(descriptionFile) {
        try {
            existingContent := FileRead(descriptionFile)
            marker := "=== ITEM DESCRIPTION ==="
            descPos := InStr(existingContent, marker)
            if (descPos) {
                newContent := SubStr(existingContent, 1, descPos + StrLen(marker)) . "`n" . description
            } else {
                newContent := existingContent . "`n" . marker . "`n" . description
            }
            if (conditionResults["messages"].Length > 0) {
                newContent .= "`n`n"
                for message in conditionResults["messages"] {
                    newContent .= message . "`n"
                }
            }
            FileDelete descriptionFile
            FileAppend newContent, descriptionFile, "UTF-8"
            LogMessage("Updated description file with processed content")
        } catch as err {
            LogMessage("Error updating description file: " . err.Message)
        }
    } else {
        LogMessage("Description file not found: " . descriptionFile)
    }
}

UpdateBaseDescriptionFile(itemNumber, descText) {
    try {
        itemDir := A_ScriptDir . "\item_contents"
        if !DirExist(itemDir)
            return
        baseFile := itemDir . "\" . itemNumber . ".txt"
        if !FileExist(baseFile)
            return
        content := FileRead(baseFile, "UTF-8")
        marker := "=== ITEM DESCRIPTION ==="
        pos := InStr(content, marker)
        if (pos) {
            newContent := SubStr(content, 1, pos + StrLen(marker)) . "`n" . descText
        } else {
            newContent := content . "`n" . marker . "`n" . descText
        }
        FileDelete baseFile
        FileAppend newContent, baseFile, "UTF-8"
        LogMessage("Updated base item file description section: " . baseFile)
    } catch as e {
        LogMessage("ERROR updating base description file: " . e.Message)
    }
}

ExtractTableHTMLLikeNewreader(itemNumber) {
    try {
        tableDir := A_ScriptDir . "\table_data"
        if !DirExist(tableDir)
            DirCreate tableDir
        savedClip := ClipboardAll()
        if (savedClip = "") {
            LogMessage("ERROR: ClipboardAll() returned empty data for table extraction")
            return
        }
        tableFile := tableDir . "\" . itemNumber . "_table.raw"
        if FileExist(tableFile)
            FileDelete tableFile
        FileAppend savedClip, tableFile, "RAW"
    } catch as e {
        LogMessage("EXCEPTION in ExtractTableHTMLLikeNewreader: " . e.Message)
    }
}

; === Robust Chrome Launch/Activate Function ===
EnsureChromeActive() {
    chromeWin := "ahk_exe chrome.exe"
    maxAttempts := 5
    currentAttempt := 0
    
    while (currentAttempt < maxAttempts) {
        ; If already active, we're done
        if WinActive(chromeWin) {
            return true
        }
        
        ; If a Chrome window exists, try to bring it to foreground
        if WinExist(chromeWin) {
            WinShow chromeWin
            WinRestore chromeWin
            WinActivate chromeWin
            if WinWaitActive(chromeWin, , 5) {
                RandomSleep(100, 300)
                return true
            }
        } else {
            ; No visible Chrome window - start one
            try {
                Run 'chrome.exe --disable-features=FocusLocationBar --no-first-run --new-window about:blank'
            } catch as e {
                ; Fallback start without flags
                Run "chrome.exe"
            }
            if WinWait(chromeWin, , 10) {
                WinActivate chromeWin
                if WinWaitActive(chromeWin, , 5) {
                    RandomSleep(100, 300)
                    return true
                }
            }
        }
        
        currentAttempt++
        RandomSleep(700, 1200)
    }
    
    return false
}

LogMessage("Starting scan_edit_specifics for item: " . itemNumber)

if (isTestMode) {
    LogMessage("TEST MODE: Using existing Chrome window; no navigation. (Open your Edit Listing tab before running)")
    if !EnsureChromeActive() {
        LogMessage("Failed to ensure Chrome active in TEST MODE.")
        SignalRestartAndExit("CHROME_ACTIVATE_FAIL_TEST")
    }
} else {
    ; Navigate directly to the eBay listing revision page
    editURL := "https://www.ebay.com/sl/list?mode=ReviseItem&itemId=" . itemNumber . "&ReturnURL=https%3A%2F%2Fwww.ebay.com%2Fsh%2Flst%2Fscheduled%3Fsort%3D-scheduledStartDate%26offset%3D100%26limit%3D50"
    LogMessage("Opening page: " . editURL)
    Run 'chrome.exe --disable-features=FocusLocationBar --no-first-run --disable-infobars "' . editURL . '"'
    Sleep 4000  ; Wait for page to load
    ; Ensure Chrome is active (robust wrapper)
    if !EnsureChromeActive() {
        LogMessage("Failed to ensure Chrome active after navigation.")
        SignalRestartAndExit("CHROME_ACTIVATE_FAIL")
    }
}

if (isTestMode) {
    Sleep 1000  ; brief pause in test mode
} else {
    ; Wait for page to load
    Sleep 1000
    ; IMPROVED: Wait for page to fully load by checking for actual content
    maxLoadWait := 30000  ; 30 seconds max wait
    loadStartTime := A_TickCount
    pageLoaded := false
    while (A_TickCount - loadStartTime < maxLoadWait) {
        CheckPause()
        ; Check page content to see if it's still loading
        A_Clipboard := ""
        Send "^a^c"
        if ClipWait(1) {
            currentContent := A_Clipboard
            ; Detect eBay invalid/expired listing page (e.g., "This is embarrassing") and skip silently
            if (RegExMatch(currentContent, "i)\bthis\s+is\s+embarr")) {
                LogMessage("Invalid/expired listing page detected ('This is embarrassing'); skipping item " . itemNumber)
                ; Create minimal placeholder files to satisfy downstream steps, then exit without popups
                try {
                    global itemDir := A_ScriptDir . "\item_contents"
                    if !DirExist(itemDir)
                        DirCreate itemDir
                    htmlFile := itemDir . "\" . itemNumber . ".html"
                    textFile := itemDir . "\" . itemNumber . ".txt"
                    if FileExist(htmlFile)
                        FileDelete htmlFile
                    if FileExist(textFile)
                        FileDelete textFile
                    placeholder := "EXPIRED / INVALID LISTING PAGE - SKIPPED"
                    FileAppend placeholder, htmlFile, "UTF-8"
                    FileAppend placeholder, textFile, "UTF-8"
                    descHtml := itemDir . "\" . itemNumber . "_description_html.txt"
                    descTxt := itemDir . "\" . itemNumber . "_description.txt"
                    if FileExist(descHtml)
                        FileDelete descHtml
                    if FileExist(descTxt)
                        FileDelete descTxt
                    FileAppend placeholder, descHtml, "UTF-8"
                    FileAppend "=== ITEM DESCRIPTION ===`n" . placeholder, descTxt, "UTF-8"
                } catch as _ {
                }
                ; Cleanup: close the edit tab before exiting (non-test mode)
                try {
                    if (!isTestMode && EnsureChromeActive()) {
                        Send "^w"
                        Sleep 300
                    }
                } catch as _ {
                }
                LogMessage("Skipping item due to invalid page content; no message box shown")
                ExitApp
            }
            ; Check if page is still showing loading placeholders
            if (InStr(currentContent, "pge--busy") || InStr(currentContent, "loading") || StrLen(currentContent) < 5000) {
                LogMessage("Page still loading, waiting... (attempt " . ((A_TickCount - loadStartTime) / 1000) . "s)")
                Sleep 2000
                continue
            } else if (InStr(currentContent, "Item specifics") || InStr(currentContent, "Revise your listing")) {
                LogMessage("Page fully loaded, proceeding with capture")
                pageLoaded := true
                break
            }
        }
        Sleep 1000
    }
    if (!pageLoaded) {
        LogMessage("WARNING: Page may not have fully loaded after " . (maxLoadWait/1000) . " seconds")
        ; If we cannot ensure Chrome is active here, request restart instead of hard fail
        if !EnsureChromeActive() {
            SignalRestartAndExit("PAGE_NOT_LOADED_AND_CHROME_INACTIVE")
        }
    }
}

; Click to focus page content
screenWidth := A_ScreenWidth
screenHeight := A_ScreenHeight
clickX := screenWidth * 0.25  ; 25% from left edge (middle-left area)
clickY := screenHeight * 0.5  ; 50% down (middle of screen)
MouseClick "left", clickX, clickY
LogMessage("Clicked to focus at MIDDLE-LEFT of screen (" . clickX . ", " . clickY . ")")
Sleep 500

; Capture content with retries
maxAttempts := 5
attempt := 0
textContent := ""
htmlContent := ""
startTime := A_TickCount
while (attempt < maxAttempts) {
    CheckPause()
    attempt++
    A_Clipboard := ""
    Send "^a"
    Sleep 100
    Send "^c"
    if ClipWait(2) {
        textContent := A_Clipboard
        htmlContent := GetClipboardHTML()
        
        ; IMPROVED: Better content validation
        isValidContent := false
        
        if (htmlContent != "" && InStr(htmlContent, "<h2>Item specifics</h2>")) {
            LogMessage("Valid HTML content captured (contains Item specifics)")
            isValidContent := true
        } else if (textContent != "" && InStr(textContent, "Item specifics")) {
            LogMessage("Valid text content captured (contains Item specifics)")
            isValidContent := true
        } else if (htmlContent != "" && InStr(htmlContent, "Revise your listing") && !InStr(htmlContent, "pge--busy")) {
            LogMessage("Valid HTML content captured (listing form without loading placeholders)")
            isValidContent := true
        }
        
        ; Additional check: reject if content is too short or contains loading indicators
        if (isValidContent) {
            if (StrLen(textContent) < 3000) {
                LogMessage("Content too short (" . StrLen(textContent) . " chars), may be incomplete")
                isValidContent := false
            } else if (InStr(textContent, "pge--busy") || InStr(htmlContent, "pge--busy")) {
                LogMessage("Content contains loading placeholders, retrying...")
                isValidContent := false
            }
        }
        
        if (isValidContent) {
            LogMessage("Captured valid content after " . attempt . " attempts")
            break
        } else {
            LogMessage("Invalid/incomplete content captured (attempt " . attempt . "), content length: " . StrLen(textContent))
        }
    }
    if (A_TickCount - startTime > 3000) {
        LogMessage("Content not found after 3s, refreshing page (attempt " . attempt . ")")
        CheckPause()
        Send "{F5}"
        Sleep 3000  ; Longer wait after refresh
        startTime := A_TickCount
    }
    CheckPause()
    Sleep 500  ; Longer delay between attempts
}

if (textContent = "" && htmlContent = "") {
    LogMessage("Failed to capture content after " . maxAttempts . " attempts")
    
    ; AGGRESSIVE RECOVERY: Try additional refresh attempts before giving up
    LogMessage("Starting aggressive recovery - multiple page refreshes")
    recoveryAttempts := 3
    
    Loop recoveryAttempts {
        recoveryAttempt := A_Index
        LogMessage("Recovery attempt " . recoveryAttempt . " of " . recoveryAttempts)
        CheckPause()
        
        ; Hard refresh with cache clear
        Send "^{F5}"  ; Ctrl+F5 for hard refresh
        Sleep 5000    ; Wait longer for complete reload
        
        ; Try to focus and capture again
        if EnsureChromeActive() {
            ; Click to focus - MIDDLE LEFT OF SCREEN
            screenWidth := A_ScreenWidth
            screenHeight := A_ScreenHeight
            clickX := screenWidth * 0.25  ; 25% from left edge (middle-left area)
            clickY := screenHeight * 0.5  ; 50% down (middle of screen)
            MouseClick "left", clickX, clickY
            Sleep 1000
            
            ; Attempt capture
            A_Clipboard := ""
            Send "^a^c"
            if ClipWait(3) {
                testContent := A_Clipboard
                testHtml := GetClipboardHTML()
                
                ; Check if we got valid content
                if (StrLen(testContent) > 3000 && !InStr(testContent, "pge--busy")) {
                    LogMessage("RECOVERY SUCCESS: Valid content captured on recovery attempt " . recoveryAttempt)
                    textContent := testContent
                    htmlContent := testHtml
                    break
                } else {
                    LogMessage("Recovery attempt " . recoveryAttempt . " failed - content length: " . StrLen(testContent))
                }
            }
        }
        
        if (recoveryAttempt < recoveryAttempts) {
            Sleep 3000  ; Wait before next recovery attempt
        }
    }
    
    ; Final check after recovery attempts
    if (textContent = "" && htmlContent = "") {
        LogMessage("CRITICAL FAILURE: All recovery attempts failed for item " . itemNumber)
        LogMessage("Requesting monitor to restart AHK due to capture failure")
        SignalRestartAndExit("CAPTURE_FAILURE_ALL_RECOVERY_FAILED")
    } else {
        LogMessage("Recovery successful - proceeding with file creation")
    }
}

; ADDITIONAL CHECK: Validate captured content isn't just loading skeleton
if (textContent != "" || htmlContent != "") {
    ; Check if the captured content is actually valid listing content
    isSkeletonContent := false
    
    if (InStr(textContent, "pge--busy") || InStr(htmlContent, "pge--busy")) {
        LogMessage("WARNING: Captured content contains loading placeholders")
        isSkeletonContent := true
    } else if (StrLen(textContent) < 1000) {
        LogMessage("WARNING: Captured content is suspiciously short (" . StrLen(textContent) . " chars)")
        isSkeletonContent := true
    } else if (InStr(textContent, "Skip to main content") && InStr(textContent, "Copyright") && StrLen(textContent) < 5000) {
        LogMessage("WARNING: Captured content appears to be eBay page skeleton only")
        isSkeletonContent := true
    }
    
    if (isSkeletonContent) {
        LogMessage("SKELETON CONTENT DETECTED - This may result in 'XX Unknown' - attempting one final recovery")
        
        ; One final aggressive attempt
        CheckPause()
        Send "^{F5}"
        Sleep 8000  ; Extended wait
        
        A_Clipboard := ""
        Send "^a^c"
        if ClipWait(3) {
            finalContent := A_Clipboard
            finalHtml := GetClipboardHTML()
            
            if (StrLen(finalContent) > 5000 && !InStr(finalContent, "pge--busy")) {
                LogMessage("FINAL RECOVERY SUCCESS: Valid content captured")
                textContent := finalContent
                htmlContent := finalHtml
            } else {
                LogMessage("FINAL RECOVERY FAILED: Item " . itemNumber . " will likely be 'XX Unknown'")
            }
        }
    }
}

; Save content to files in item_contents directory with correct naming
global itemDir := A_ScriptDir . "\item_contents"
if !DirExist(itemDir)
    DirCreate itemDir

if (isTestMode) {
    htmlFile := itemDir . "\\test description.html"
    textFile := itemDir . "\\test description.txt"
} else {
    htmlFile := itemDir . "\" . itemNumber . ".html"
    textFile := itemDir . "\" . itemNumber . ".txt"
}

; Save HTML content
if (htmlContent != "") {
    FileAppend htmlContent, htmlFile, "UTF-8"
    LogMessage("Saved HTML content to " . htmlFile)
} else {
    FileAppend textContent, htmlFile, "UTF-8"
    LogMessage("Saved text content to " . htmlFile . " (HTML not available)")
}

; Save plaintext content
if (textContent != "") {
    FileAppend textContent, textFile, "UTF-8"
    LogMessage("Saved plaintext content to " . textFile)
} else {
    LogMessage("No plaintext content to save")
}

; Try to specifically capture the Description editor via TAB scan and save to *_description_* files
CaptureDescriptionByTabScanning(itemNumber)

; Verify files exist (silent)
if !FileExist(htmlFile) {
    LogMessage("HTML file not created: " . htmlFile)
    ExitApp
}
if !FileExist(textFile) {
    LogMessage("Text file not created: " . textFile)
    ExitApp
}

; Run the Python script with the captured files
pythonScript := A_ScriptDir . "\extract_specifics.py"
if !FileExist(pythonScript) {
    LogMessage("Python script not found: " . pythonScript)
    ExitApp
}
command := 'python "' . pythonScript . '" "' . htmlFile . '" "' . textFile . '"'
LogMessage("Running Python script: " . command)
try {
    RunWait command, , , &pid
    LogMessage("Python script completed with PID: " . pid)
} catch as e {
    LogMessage("Error running Python script: " . e.Message)
    ExitApp
}

; Final normalization: if description HTML mirror exists, process it like newreader (runs AFTER Python step)
try {
    finalDescHtmlPath := itemDir . "\\" . itemNumber . "_description_html.txt"
    if FileExist(finalDescHtmlPath) {
        finalHtml := FileRead(finalDescHtmlPath, "UTF-8")
        ProcessDescriptionContent(finalHtml, itemNumber)
        LogMessage("Post-Python normalization completed for description file")
    }
} catch as e {
    LogMessage("ERROR during post-Python normalization: " . e.Message)
}

; Cleanup: close the edit tab when done (non-test mode)
try {
    if (!isTestMode) {
        ; Ensure Chrome is active before sending Ctrl+W; if not, request restart instead of erroring
        if EnsureChromeActive() {
            Send "^w"
            Sleep 300
        } else {
            SignalRestartAndExit("CHROME_INACTIVE_AT_CLEANUP")
        }
    }
} catch as _ {
}
LogMessage("scan_edit_specifics completed for item: " . itemNumber)
ExitApp