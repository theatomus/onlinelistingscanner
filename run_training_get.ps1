# 1) Pick an item and derive the item number
$first = (Get-ChildItem .\item_contents -Filter 'python_parsed_*.txt' | Select-Object -First 1).Name
$inum  = ($first -replace '^python_parsed_','') -replace '\.txt$',''

# 2) Generate the AI issues file (this creates training\live_issues\<item>.txt)
python -m tools.training.write_ai_issues $inum --items-dir .\item_contents --schema .\training\schema.json --lm .\training\mini_lm.json --llm-url http://127.0.0.1:8080 --web-verify

# 3) View it
Get-Content ("training\live_issues\{0}.txt" -f $inum)