#Requires AutoHotkey v2.0

; Include GDI+ library for screenshot functionality
#Include "Lib\Gdip_All.ahk"

; === Config: toggle description screenshots ===
global ENABLE_SCREENSHOTS := false  ; Set to true to enable screenshots

; Basic setup
SetWorkingDir A_ScriptDir
; Create centralized logs directory structure
global logsDir := A_ScriptDir . "\logs"
global processingLogsDir := logsDir . "\processing"
global logDir := processingLogsDir . "\pull_logs"
if !DirExist(logsDir)
    DirCreate logsDir
if !DirExist(processingLogsDir)
    DirCreate processingLogsDir
if !DirExist(logDir)
    DirCreate logDir
if !DirExist(logDir)
    DirCreate logDir
global errorLogFile := logDir . "\script_error.txt"

; Check Shift key to determine itemNumber input mode
if GetKeyState("Shift", "P") {
    clipboardContent := Trim(A_Clipboard)
    if (clipboardContent = "" || !RegExMatch(clipboardContent, "^[a-zA-Z0-9]{1,20}$")) {
        FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Invalid clipboard content for item number`n", errorLogFile
        ExitApp
    }
    itemNumber := clipboardContent
    FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Using item number from clipboard: " . itemNumber . "`n", errorLogFile
} else {
    if (A_Args.Length = 0) {
        FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Usage: script.ahk <itemNumber>`n", errorLogFile
        ExitApp
    }
    itemNumber := A_Args[1]
    FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Using item number from command line: " . itemNumber . "`n", errorLogFile
}

; Define item-specific log file
global itemLogFile := logDir . "\" . itemNumber . "_pull_log.txt"
if FileExist(itemLogFile)
    FileDelete itemLogFile  ; Delete existing log to start fresh

; Continue with script setup
SendMode "Input"
CoordMode "Mouse", "Screen"
ProcessSetPriority "High"

; Detect screen resolution
screenWidth := A_ScreenWidth
screenHeight := A_ScreenHeight
FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Detected screen resolution: " . screenWidth . "x" . screenHeight . "`n", itemLogFile

; ### Global Pause Variables
global isPaused := false
global wasPaused := false  ; Tracks the previous pause state for logging

; ### Helper Functions for Human-like Behavior
RandomSleep(min, max) {
    Sleep Random(min, max)
}

HumanWaitForPage() {
    if CheckPause()
        return true
    RandomSleep(1893, 3758)
    return false
}

HumanActionDelay() {
    if CheckPause()
        return true
    RandomSleep(200, 800)
    return false
}

TabSwitchDelay() {
    if CheckPause()
        return true
    RandomSleep(600, 1200)
    return false
}

; ### Hotkeys for Pausing
Hotkey "^m", PauseScript  ; Ctrl+M to toggle pause
Hotkey "^q", PauseScript  ; Ctrl+Q to toggle pause

PauseScript(*) {
    global isPaused
    isPaused := !isPaused  ; Toggle pause state without logging (logging is handled in CheckPause)
}

; ### Updated CheckPause Function
CheckPause() {
    global isPaused, wasPaused, itemLogFile
    currentPaused := isPaused || (GetKeyState("CapsLock", "T") && !GetKeyState("Shift"))
    
    ; Log when the pause state changes
    if (currentPaused != wasPaused) {
        if (currentPaused) {
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Script paused`n", itemLogFile
        } else {
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Script resumed`n", itemLogFile
        }
        wasPaused := currentPaused
    }
    
    ; Pause while the condition is true
    while (currentPaused) {
        Sleep 100
        currentPaused := isPaused || (GetKeyState("CapsLock", "T") && !GetKeyState("Shift"))
    }
    return false
}

; ### Robust Chrome Launch/Activate Function
EnsureChromeActive() {
	if CheckPause()
		return false
	chromeWin := "ahk_exe chrome.exe"
	maxAttempts := 5
	currentAttempt := 0
	while (currentAttempt < maxAttempts) {
		if WinActive(chromeWin) {
			return true
		}
		if WinExist(chromeWin) {
			WinShow chromeWin
			WinRestore chromeWin
			WinActivate chromeWin
			if WinWaitActive(chromeWin, , 5)
				return true
		} else {
			; No visible window - launch Chrome
			try {
				Run 'chrome.exe --disable-features=FocusLocationBar --no-first-run --new-window about:blank'
			} catch as e {
				Run "chrome.exe"
			}
			if WinWait(chromeWin, , 10) {
				WinActivate chromeWin
				if WinWaitActive(chromeWin, , 5)
					return true
			}
		}
		currentAttempt++
		RandomSleep(700, 1200)
	}
	FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Failed to activate or launch Chrome after retries`n", itemLogFile
	return false
}

; ### Create Directories
global itemDir := A_ScriptDir . "\item_contents"
if !DirExist(itemDir)
    DirCreate itemDir

global tableDir := A_ScriptDir . "\table_data"
if !DirExist(tableDir)
    DirCreate tableDir

global screenshotDir := A_ScriptDir . "\description_screenshots"
if (ENABLE_SCREENSHOTS) {
    if !DirExist(screenshotDir)
        DirCreate screenshotDir
}

; ### Helper Functions
ExtractTableHTML(itemNumber) {
    if CheckPause()
        return
    tableDir := A_ScriptDir . "\table_data"
    if !DirExist(tableDir)
        DirCreate tableDir

    try {
        savedClip := ClipboardAll()
        if (savedClip = "") {
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - ERROR: ClipboardAll() returned empty data for item: " . itemNumber . "`n", itemLogFile
            return
        }
        
        tableFile := tableDir . "\" . itemNumber . "_table.raw"
        if FileExist(tableFile) {
            FileDelete tableFile
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Deleted existing table file: " . tableFile . "`n", itemLogFile
        }
        
        FileAppend savedClip, tableFile, "RAW"
        
        ; Verify file was created and has content
        if FileExist(tableFile) {
            fileSize := FileGetSize(tableFile)
            if (fileSize > 0) {
                FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Successfully created table raw file: " . tableFile . " (size: " . fileSize . " bytes)`n", itemLogFile
            } else {
                FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - WARNING: Table raw file created but is empty: " . tableFile . "`n", itemLogFile
            }
        } else {
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - ERROR: Failed to create table raw file: " . tableFile . "`n", itemLogFile
        }
    } catch Error as err {
        FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - EXCEPTION in ExtractTableHTML for item " . itemNumber . ": " . err.Message . "`n", itemLogFile
    }
}

ExtractTextFromHTML(htmlContent) {
    ; Remove HTML tags but preserve some structure
    cleanText := htmlContent
    
    ; Convert common HTML entities
    cleanText := StrReplace(cleanText, "&amp;", "&")
    cleanText := StrReplace(cleanText, "&lt;", "<")
    cleanText := StrReplace(cleanText, "&gt;", ">")
    cleanText := StrReplace(cleanText, "&quot;", '"')
    cleanText := StrReplace(cleanText, "&#39;", "'")
    cleanText := StrReplace(cleanText, "&nbsp;", " ")
    
    ; Replace paragraph and break tags with newlines
    cleanText := RegExReplace(cleanText, "i)</?p[^>]*>", "`n")
    cleanText := RegExReplace(cleanText, "i)<br[^>]*>", "`n")
    cleanText := RegExReplace(cleanText, "i)</div>", "`n")
    
    ; Remove all remaining HTML tags
    cleanText := RegExReplace(cleanText, "<[^>]*>", "")
    
    ; Clean up extra whitespace and newlines
    cleanText := RegExReplace(cleanText, "[ \t]+", " ")
    cleanText := RegExReplace(cleanText, "\n[ \t]*\n", "`n")
    cleanText := RegExReplace(cleanText, "^\s+|\s+$", "")
    
    return cleanText
}

ProcessDescriptionContent(htmlContent, itemNumber) {
    if CheckPause()
        return
        
    FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Processing description content for item: " . itemNumber . "`n", itemLogFile
    
    ; Extract readable text from HTML content
    cleanText := ExtractTextFromHTML(htmlContent)
    
    ; Check conditions and extract description
    conditionResults := CheckConditions(cleanText)
    description := TruncateTableContent(conditionResults["cleanedDesc"])
    
    ; Update the description file
    descriptionFile := itemDir . "\" . itemNumber . "_description.txt"
    if FileExist(descriptionFile) {
        try {
            existingContent := FileRead(descriptionFile)
            
            ; Find the ITEM DESCRIPTION section
            descPos := InStr(existingContent, "=== ITEM DESCRIPTION ===")
            
            if (descPos) {
                ; Replace the description section
                newContent := SubStr(existingContent, 1, descPos + StrLen("=== ITEM DESCRIPTION ===")) . "`n" . description
            } else {
                ; Append description section
                newContent := existingContent . "`n=== ITEM DESCRIPTION ===`n" . description
            }
            
            ; Add condition messages if any
            if (conditionResults["messages"].Length > 0) {
                newContent .= "`n`n"
                for message in conditionResults["messages"] {
                    newContent .= message . "`n"
                }
            }
            
            ; Write updated content
            FileDelete descriptionFile
            FileAppend newContent, descriptionFile
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Updated description file with processed content`n", itemLogFile
            
        } catch as err {
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Error updating description file: " . err.Message . "`n", itemLogFile
        }
    } else {
        FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Description file not found: " . descriptionFile . "`n", itemLogFile
    }
}

CheckConditions(descText) {
    if CheckPause()
        return Map("hasError", false, "hasWarning", false, "messages", [], "cleanedDesc", descText)
        
    result := Map()
    result["hasError"] := false
    result["hasWarning"] := false
    result["messages"] := []
    
    ; Handle disclaimer removal
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
    
    descText := result["cleanedDesc"]
    descText := Trim(descText)
    lines := StrSplit(descText, "`n", "`r")
    
    ; Check for Cosmetic Condition
    cosmeticFound := false
    for _, line in lines {
        if (RegExMatch(line, "i).*(C[2-6]|C8|C9|C10)\s*-\s*.*", &match) || RegExMatch(line, "i).*\bCosmetic\s*Condition\b.*", &match)) {
            cosmeticFound := true
            break
        }
    }
    if (!cosmeticFound) {
        result["messages"].Push("INFO: Cosmetic Condition not found")
    }
    
    ; Check for Functional Condition
    functionalFound := false
    for _, line in lines {
        if (RegExMatch(line, "i).*\bFunctional\s*Condition\b.*", &match)) {
            functionalFound := true
            break
        }
    }
    if (!functionalFound) {
        ; Look for condition code pattern in the entire description
        if (RegExMatch(descText, "i)\bF[1-6]|F10\b[^\w]*\w+", &match)) {
            functionalFound := true
            result["messages"].Push("INFO: Functional Condition detected via condition code pattern")
        } else {
            result["messages"].Push("WARNING: Functional Condition not found - manual review required")
        }
    }
    
    ; Check for Data Sanitization
    dataSanitizationFound := false
    for _, line in lines {
        if (RegExMatch(line, "i).*(no\s*data|non[- ]*data|data\s*sanitization).*", &match)) {
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
    if CheckPause()
        return content
    tablePos := InStr(content, "Make	Model	CPU")
    if (tablePos > 0) {
        return Trim(SubStr(content, 1, tablePos - 1))
    }
    return content
}

TakeDescriptionScreenshot(itemNumber) {
    if CheckPause()
        return false
    
    if (!ENABLE_SCREENSHOTS)
        return false
        
    FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Taking screenshot of description page for item: " . itemNumber . "`n", itemLogFile
    
    try {
        ; Ensure Chrome is active and the window is ready
        if !EnsureChromeActive() {
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Failed to activate Chrome for screenshot`n", itemLogFile
            return false
        }
        
        ; Get active window position and size
        WinGetPos &x, &y, &width, &height, "A"
        
        ; Create the screenshot file path using the item number
        screenshotFile := screenshotDir . "\" . itemNumber . "_description.png"
        
        ; Take screenshot using GDI+ library
        if FileExist(screenshotFile)
            FileDelete screenshotFile
        
        ; Capture the screenshot with GDI+
        pToken := Gdip_Startup()
        pBitmap := Gdip_BitmapFromScreen(x . "|" . y . "|" . width . "|" . height)
        Gdip_SaveBitmapToFile(pBitmap, screenshotFile)
        Gdip_DisposeImage(pBitmap)
        Gdip_Shutdown(pToken)
        
        ; Check if the screenshot was created successfully
        if FileExist(screenshotFile) {
            fileSize := FileGetSize(screenshotFile)
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Screenshot saved to: " . screenshotFile . " (size: " . fileSize . " bytes)`n", itemLogFile
            return true
        } else {
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Failed to save screenshot to: " . screenshotFile . "`n", itemLogFile
            return false
        }
    } catch Error as err {
        FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - EXCEPTION in TakeDescriptionScreenshot: " . err.Message . "`n", itemLogFile
        return false
    }
}

; ### Main Script Logic - Description URL Download Only
FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Starting description download for item: " . itemNumber . "`n", itemLogFile

; Close current tab first (if Chrome is already open)
if WinExist("ahk_exe chrome.exe") {
    WinActivate "ahk_exe chrome.exe"
    RandomSleep(500, 1000)
    Send "^w"  ; Close current tab
    FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Closed current tab before opening description page`n", itemLogFile
    RandomSleep(500, 1000)
        }

; Navigate directly to description URL
            descURL := "https://itm.ebaydesc.com/itmdesc/" . itemNumber . "?t=0&seller=techredosurplus&excSoj=1&ver=0&excTrk=1&lsite=0&ittenable=true&domain=ebay.com&descgauge=1&cspheader=1&oneClk=2&secureDesc=1"
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Navigating to description URL: " . descURL . "`n", itemLogFile
            
; Open new tab with the description URL
if WinExist("ahk_exe chrome.exe") {
    ; Chrome is already open, ensure it's active then open URL directly to avoid focus issues
    if !WinActive("ahk_exe chrome.exe") {
        WinActivate "ahk_exe chrome.exe"
        WinWaitActive "ahk_exe chrome.exe", , 3
    }
    Run 'chrome.exe --new-tab "' . descURL . '"'
} else {
    ; Start Chrome and navigate to the description URL
    Run 'chrome.exe --no-first-run --new-window "' . descURL . '"'
}
Sleep Random(3000, 5000)  ; Wait for Chrome to start and page to load

if EnsureChromeActive() {
    WinMaximize "ahk_exe chrome.exe"
            
            ; Extended wait for page to load with variable timing - increased for dynamic content
            humanPageLoadWait := Random(5000, 8000)  ; Increased from 3-6 to 5-8 seconds
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Waiting " . humanPageLoadWait . "ms for description page to load`n", itemLogFile
            Sleep humanPageLoadWait
            
            ; Additional human behavior - check if page loaded properly with retries
            maxLoadAttempts := 4  ; Increased from 3 to 4 attempts
            currentLoadAttempt := 0
            pageReady := false
            
            while (currentLoadAttempt < maxLoadAttempts && !pageReady) {
                currentLoadAttempt++
                
                ; Ensure Chrome is still active
                if !EnsureChromeActive() {
            FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Chrome not active during content capture (attempt " . currentLoadAttempt . ")`n", itemLogFile
                    RandomSleep(1000, 2000)
                    continue
                }
                
                ; IMPORTANT: Click on the page to ensure it's focused and content is accessible
                Click 400, 300  ; Click in center area of page to activate content
                RandomSleep(300, 600)  ; Wait after click
                FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Clicked on page to ensure focus (attempt " . currentLoadAttempt . ")`n", itemLogFile
                
                ; Take a screenshot of the description page with table (first attempt is best timing)
                if (ENABLE_SCREENSHOTS && currentLoadAttempt = 1) {
                    TakeDescriptionScreenshot(itemNumber)
                }
                
                ; Human-like content capture with multiple attempts
                RandomSleep(800, 1500)  ; Increased pause before trying to select content
                
                ; Clear clipboard with validation
                A_Clipboard := ""
                RandomSleep(400, 700)  ; Increased pause after clearing clipboard
                
                ; Ensure clipboard is actually cleared before proceeding
                ClipWait(0.5, 0)  ; Wait for clipboard to be empty
                
                ; Select all content
                Send "^a"
                RandomSleep(600, 1200)  ; Increased human pause between select and copy
                
                ; Copy content
                Send "^c"
                
                ; Wait for clipboard with increased patience for complex pages
                clipWaitTime := Random(3000, 6000)  ; Increased from 2-4 to 3-6 seconds
                clipSuccess := ClipWait(clipWaitTime / 1000)
                
                if (clipSuccess && A_Clipboard != "") {
                    ; Enhanced validation for meaningful content
                    clipLength := StrLen(A_Clipboard)
                    ; Check for HTML-like content or eBay-specific patterns
                    hasHTMLContent := InStr(A_Clipboard, "<") && InStr(A_Clipboard, ">")
                    haseBayContent := InStr(A_Clipboard, "ebay") || InStr(A_Clipboard, "Make") || InStr(A_Clipboard, "Model")
                    
                    if (clipLength > 50) {  ; Simplified content validation - just check for reasonable length
                        pageReady := true
                        FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Successfully captured description content (attempt " . currentLoadAttempt . ", length: " . clipLength . ", HTML: " . (hasHTMLContent ? "Yes" : "No") . ", eBay: " . (haseBayContent ? "Yes" : "No") . ")`n", itemLogFile
                        
                        ; Take another screenshot if first one may have failed
                        if (ENABLE_SCREENSHOTS && currentLoadAttempt > 1) {
                            screenshotFile := screenshotDir . "\" . itemNumber . "_description.png"
                            if !FileExist(screenshotFile) {
                                FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Taking fallback screenshot after successful content capture`n", itemLogFile
                                TakeDescriptionScreenshot(itemNumber)
                            }
                        }
                
                ; Save the HTML content to file
                htmlFile := itemDir . "\" . itemNumber . "_description_html.txt"
                if FileExist(htmlFile)
                    FileDelete htmlFile
                FileAppend A_Clipboard, htmlFile
                FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - HTML content saved to: " . htmlFile . "`n", itemLogFile
                
                ; Also save table data - with enhanced logging
                FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Attempting to extract table HTML data`n", itemLogFile
                ExtractTableHTML(itemNumber)
                
                ; Verify table file was created
                tableFile := tableDir . "\" . itemNumber . "_table.raw"
                if FileExist(tableFile) {
                    tableSize := FileGetSize(tableFile)
                    FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Table raw file created successfully, size: " . tableSize . " bytes`n", itemLogFile
                } else {
                    FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - WARNING: Table raw file was not created`n", itemLogFile
                }
                
                ; Process the description content and update the description file
                ProcessDescriptionContent(A_Clipboard, itemNumber)
                
                    } else {
                        FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Content validation failed (attempt " . currentLoadAttempt . ", length: " . clipLength . ", HTML: " . (hasHTMLContent ? "Yes" : "No") . ", eBay: " . (haseBayContent ? "Yes" : "No") . ")`n", itemLogFile
                    }
                } else {
                    FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Failed to capture content on attempt " . currentLoadAttempt . " (ClipWait: " . (clipSuccess ? "Success" : "Failed") . ", ClipLength: " . StrLen(A_Clipboard) . ")`n", itemLogFile
                }
                
                ; If not ready and more attempts remain, wait before retry
                if (!pageReady && currentLoadAttempt < maxLoadAttempts) {
                    retryWait := Random(3000, 6000)  ; Increased retry wait time
                    FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Waiting " . retryWait . "ms before retry`n", itemLogFile
                    Sleep retryWait
                    
                    ; Try scrolling and refreshing on different attempts
                    if (currentLoadAttempt = 2) {
                        FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Scrolling page to load dynamic content`n", itemLogFile
                        Send "{End}"  ; Scroll to bottom to trigger any lazy loading
                        RandomSleep(1000, 2000)
                        Send "{Home}" ; Scroll back to top
                        RandomSleep(1000, 2000)
                    } else if (currentLoadAttempt = 3) {
                        FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Refreshing page on third retry attempt`n", itemLogFile
                        Send "{F5}"
                        RandomSleep(5000, 8000)  ; Wait longer after refresh for dynamic content
                    }
                }
            }
            
    if (!pageReady) {
        FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Failed to capture description content after " . maxLoadAttempts . " attempts for item: " . itemNumber . "`n", itemLogFile
            }
            
    ; Human-like closing behavior
    RandomSleep(800, 1500)  ; Pause before closing
            Send "^w"
            if TabSwitchDelay()
        return
        
} else {
    FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Failed to activate Chrome for description download`n", itemLogFile
}

FileAppend FormatTime(A_Now, "yyyy-MM-dd HH:mm:ss") . " - Description download completed for item: " . itemNumber . "`n", itemLogFile
ExitApp