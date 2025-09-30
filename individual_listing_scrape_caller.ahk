#Requires AutoHotkey v2.0

; CapsLock pause functionality
CheckPause() {
    while (GetKeyState("CapsLock", "T")) {
        Sleep 100
    }
}

; Define Logger class
class Logger {
    static __New() {
        ; Create logs directory structure if it doesn't exist
        logsDir := A_ScriptDir . "\logs"
        individualLogsDir := logsDir . "\individual"
        if !DirExist(logsDir)
            DirCreate logsDir
        if !DirExist(individualLogsDir)
            DirCreate individualLogsDir
        this.logFile := individualLogsDir . "\debug.log"
    }
    
    static Log(message, source := "") {
        timestamp := FormatTime(, "yyyy-MM-dd HH:mm:ss")
        logEntry := timestamp . " [" . source . "] " . message . "`n"
        try {
            FileAppend(logEntry, this.logFile)
        }
        catch Error as err {
            ; If we can't write to the log file, at least show a tooltip
            ToolTip("Logging error: " . err.Message)
            SetTimer(() => ToolTip(), -3000)  ; Hide tooltip after 3 seconds
        }
    }
}

; Define folder variables
global EBAY_DATA_DIR := A_ScriptDir . "\eBayListingData"
global PROCESSED_ITEMS_DIR := EBAY_DATA_DIR
global STATE_DIR := A_ScriptDir . "\state"

; Input file paths
activeInputFile := EBAY_DATA_DIR . "\all_item_numbers_active.txt"
scheduledInputFile := EBAY_DATA_DIR . "\all_item_numbers_scheduled.txt"
; Ensure state directory exists
if !DirExist(STATE_DIR)
    DirCreate STATE_DIR

; Output file path
outputFile := STATE_DIR . "\items_to_scan.txt"
; Blacklist file path (canonical moved to state dir)
blacklistFile := STATE_DIR . "\processed_items_blacklist.txt"

; Global function: check if an item is in the blacklist (usable in all modes)
IsItemBlacklisted(item) {
	global blacklistFile
	if (!FileExist(blacklistFile)) {
		return false
	}
	try {
		Loop Read, blacklistFile {
			if (Trim(A_LoopReadLine) == item) {
				return true
			}
		}
	} catch Error as err {
		Logger.Log("Error checking blacklist: " . err.Message, "Main")
	}
	return false
}

; AI assistance toggle
global EnableAI := false  ; When false, skip AI helper steps (issues, ai_python_parsed, continuous learning)
; Setting for real-time processing
global RealTimeProcessing := true  ; Set to true or false as needed
global ContinuousLearning := false  ; When true (and EnableAI), update instructions/schema/mini‑LM after each item

; Check if caps lock is on for single item processing
global singleItemMode := GetKeyState("CapsLock", "T")

if (singleItemMode) {
    ; Single item mode - use clipboard content
    clipboardContent := Trim(A_Clipboard)
    if (clipboardContent = "" || !RegExMatch(clipboardContent, "^[a-zA-Z0-9]{1,20}$")) {
        Logger.Log("Invalid clipboard content for item number: '" . clipboardContent . "'", "Main")
        ExitApp
    }
    singleItem := clipboardContent
    Logger.Log("Single item mode enabled via CapsLock. Processing item: " . singleItem, "Main")
    
    ; Create items array with just the single item
    allItems := [singleItem]
} else {
    ; Normal mode - processing files
    Logger.Log("Normal mode - processing files", "Main")

    ; Function to process file and extract items
    ProcessFile(filePath, maxItems) {
        if !FileExist(filePath) {
            Logger.Log("File not found: " . filePath, "ProcessFile")
            return []
        }
            
        try {
            content := FileRead(filePath)
        } catch Error as err {
            Logger.Log("Error reading file: " . err.Message, "ProcessFile")
            return []
        }
        
        items := []
        itemCount := 0
        
        loop Parse, content, "`n", "`r" {
            if InStr(A_LoopField, "Item: ") {
                ; Find where "Item: " starts in the line
                itemStart := InStr(A_LoopField, "Item: ")
                ; Compute the length of "Item: " to move past it dynamically
                itemLength := StrLen("Item: ")
                ; Extract only the number after "Item: "
                itemText := Trim(SubStr(A_LoopField, itemStart + itemLength))
                
                ; Check if the item is not in the blacklist before adding
                if (!IsItemBlacklisted(itemText)) {
                    items.Push(itemText)
                    itemCount++
                }
                
                ; Break the loop if we have reached maxItems
                if (itemCount >= maxItems)
                    break
            }
        }
        return items
    }

    ; Process files and get items
    activeItems := ProcessFile(activeInputFile, 40)
    scheduledItems := ProcessFile(scheduledInputFile, 100)

    ; Combine active and scheduled items with in-order deduplication
    allItems := []
    seen := Map()
    for item in activeItems {
        if (!seen.Has(item)) {
            allItems.Push(item)
            seen[item] := true
        }
    }
    for item in scheduledItems {
        if (!seen.Has(item)) {
            allItems.Push(item)
            seen[item] := true
        }
    }
}

; Avoid spawning an empty New Tab on every cycle: only open a blank tab if Chrome isn't already open
if !WinExist("ahk_exe chrome.exe")
	Run "chrome.exe --no-first-run about:blank"

if FileExist(outputFile)
    FileDelete(outputFile)

; Process all items (either single item or multiple items from files)
processedThisRun := Map()
for item in allItems {
    CheckPause()  ; Check for pause before processing each item
    
    ; Runtime blacklist enforcement
    if (IsItemBlacklisted(item)) {
        Logger.Log("Skipped blacklisted item at execution: " . item, "Main")
        continue
    }

    ; In-run deduplication guard
    if (processedThisRun.Has(item)) {
        Logger.Log("Skipped duplicate in same run: " . item, "Main")
        continue
    }
    processedThisRun[item] := true

    ; Ensure the output file exists before appending
    if (!FileExist(outputFile)) {
        try {
            FileAppend("", outputFile)  ; Create file if it doesn't exist
        } catch Error as err {
            Logger.Log("Error creating output file: " . err.Message, "Main")
            continue
        }
    }
    
    try {
        FileAppend(item . "`n", outputFile)  ; Append item followed by a newline
        
        ; Run scripts for current item
        CheckPause()
        Logger.Log("Running scan_edit_specifics.ahk for item: " . item, "Main")
        RunWait('"' . A_ScriptDir . '\scan_edit_specifics.ahk" "' . item . '"')
        CheckPause()
        ; Logger.Log("Running newreader.ahk for item: " . item, "Main")
        ; RunWait('"' . A_ScriptDir . '\newreader.ahk" "' . item . '"')
        ; CheckPause()
        Logger.Log("Running convert_table.ahk for item: " . item, "Main")
        RunWait('"' . A_ScriptDir . '\convert_table.ahk" "' . item . '"')
        
        ; If real-time processing is enabled, run Python scripts for this item
        if (RealTimeProcessing) {
            CheckPause()
            Logger.Log("Running process_description.py for item: " . item, "Main")
            RunWait('python "' . A_ScriptDir . '\process_description.py" "' . item . '" --skip-runit')
            CheckPause()
            Logger.Log("Running runit.py for item: " . item, "Main")
            RunWait('python "' . A_ScriptDir . '\runit.py" "' . item . '"')
            ; Optional AI assistance steps
            if (EnableAI) {
                ; NEW: Generate AI issues file for RunIt tab (uses local LLM if available)
                CheckPause()
                Logger.Log("Generating AI issues for item: " . item, "Main")
                RunWait('python "' . A_ScriptDir . '\tools\training\write_ai_issues.py" "' . item . '" --items-dir "' . A_ScriptDir . '\item_contents" --schema "' . A_ScriptDir . '\training\schema.json" --lm "' . A_ScriptDir . '\training\mini_lm.json" --llm-url http://127.0.0.1:8080 --fallback-url http://127.0.0.1:8081')

                ; Generate AI python parsed for this item (optional LLM)
                try {
                    Logger.Log("Generating AI python parsed for item: " . item, "Main")
                    RunWait('python "' . A_ScriptDir . '\tools\training\ai_python_parsed.py" "' . item . '" --items-dir "' . A_ScriptDir . '\item_contents" --llm-url http://127.0.0.1:8080')
                } catch Error as err {
                    Logger.Log("AI parsed generation error: " . err.Message, "Main")
                }
            }

            ; Continuous learning pipeline (runs in background with a lock)
            if (EnableAI && ContinuousLearning) {
                CheckPause()
                try {
                    Logger.Log("Starting background continuous learning", "Main")
                    Run('python "' . A_ScriptDir . '\tools\training\continuous_learning_runner.py" --items-dir "' . A_ScriptDir . '\item_contents" --backups-root "' . A_ScriptDir . '\backups\itemcontents" --training-dir "' . A_ScriptDir . '\training" --instructions "' . A_ScriptDir . '\training\config\instructions.yaml"')
                } catch Error as err {
                    Logger.Log("continuous_learning_runner error: " . err.Message, "Main")
                }
            }
        }
        
        ; Add item to blacklist after processing (only in normal mode)
        if (!singleItemMode) {
            FileAppend(item . "`n", blacklistFile)
            Logger.Log("Added item to blacklist: " . item, "Main")
        }
    } catch Error as err {
        Logger.Log("Error processing item " . item . ": " . err.Message, "Main")
    }
}

; Run final processing only if real-time processing is disabled
if (!RealTimeProcessing) {
    try {
        Logger.Log("Running final process_description.py", "Main")
        RunWait(A_ScriptDir . "\process_description.py")
        Logger.Log("Running final runit.py", "Main")
        RunWait(A_ScriptDir . "\runit.py")
        sleepTime := Random(1000, 5000)
        Sleep(sleepTime)
    } catch Error as err {
        Logger.Log("Error running final processing: " . err.Message, "Main")
    }
}

; Terminate the script
ExitApp