# pyright: ignore-all-errors
import sys
import os
import traceback
from datetime import datetime
import re

# Setup paths
script_dir = os.path.dirname(os.path.abspath(__file__))
# Create centralized logs directory structure
logs_dir = os.path.join(script_dir, "logs")
processing_logs_dir = os.path.join(logs_dir, "processing")
pull_logs_dir = os.path.join(processing_logs_dir, "pull_logs")
os.makedirs(pull_logs_dir, exist_ok=True)
log_file = os.path.join(script_dir, "python_log.txt")  # Default, will be overridden per item

try:
    from bs4 import BeautifulSoup
    import re
except ImportError as e:
    print(f"Import error: {e}")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Import error: {e}\n")
    sys.exit(1)

def log_message(message, console=True):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{current_time} - {message}"
    if console:
        print(log_entry)
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_entry + "\n")
    except Exception as e:
        print(f"Failed to write to log file {log_file}: {e}")

def extract_category_from_plaintext(text_content):
    lines = text_content.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        if line.lower() == 'item category' or 'item category' in line.lower():
            for j in range(i + 1, min(i + 5, len(lines))):
                next_line = lines[j].strip()
                if not next_line or next_line.lower() in ['edit', 'required', 'optional']:
                    continue
                leaf_category = next_line
                path = ""
                for k in range(j + 1, min(j + 3, len(lines))):
                    path_line = lines[k].strip()
                    if path_line.startswith('in '):
                        path = path_line[3:].strip()
                        break
                if path:
                    full_category = f"{path} > {leaf_category}"
                    log_message(f"Found category - Leaf: '{leaf_category}', Path: '{path}', Combined: '{full_category}'")
                    return full_category
                else:
                    log_message(f"Found category leaf only: '{leaf_category}'")
                    return leaf_category
    for line in lines:
        line = line.strip()
        if line.startswith('in ') and ('>' in line or '&' in line):
            path = line[3:].strip()
            log_message(f"Found fallback category path: '{path}'")
            return path
    log_message("No category information found in plaintext")
    return 'Unknown'

def extract_specifics_from_plaintext(text_content):
    lines = text_content.split('\n')
    specifics = {}
    in_specifics = False
    in_required = False
    in_additional = False
    current_key = None
    placeholders = ['Enter number', 'Enter your own', '', 'See details']  # Updated: Added 'See details'
    skip_patterns = [
        r'Buyers (need|also search for) these details\.?',
        r'~.*searches',
        r'Frequently selected.*',
        r'Select all',
        r'^\d+/\d+$',
        r'^in\..*',
        r'^oz\.',
        r'^Trending$'
    ]

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if any(re.match(pattern, line, re.I) for pattern in skip_patterns) or line in [
            'Buyers need these details to find your item.',
            'Buyers also search for these details.',
            'Show more'
        ]:
            log_message(f"Skipped subtext/noise: '{line}'")
            continue

        if 'Item specifics' in line:
            in_specifics = True
            continue

        if not in_specifics:
            continue

        if re.match(r'(Required|Mandatory|Essential).*', line, re.I):
            in_required = True
            in_additional = False
            continue

        if re.match(r'(Additional|Optional).*', line, re.I):
            in_required = False
            in_additional = True
            continue

        if any(x in line for x in ['Variations', 'Learn more', 'Condition']):
            break

        if in_required or in_additional:
            if re.match(r'^[A-Z][a-zA-Z\s\(\)]+$', line):
                if current_key:
                    log_message(f"Dropping unpaired key '{current_key}' due to new key '{line}'")
                current_key = line
            elif current_key:
                if line not in placeholders and not any(re.match(p, line, re.I) for p in skip_patterns):
                    specifics[current_key] = line
                    log_message(f"Plaintext extracted: {current_key}: {line}")
                    current_key = None
                else:
                    log_message(f"Skipped placeholder value for key '{current_key}': {line}")
                    current_key = None
            else:
                log_message(f"Skipped line '{line}' - no current key")

    return specifics

def extract_from_text(text_content, section_marker, value_offset=1, post_process=None):
    lines = text_content.split('\n')
    for i, line in enumerate(lines):
        if re.match(rf'^{re.escape(section_marker)}(\(optional\))?$', line, re.I):
            for j in range(1, value_offset + 5):
                if i + j < len(lines):
                    potential_value = lines[i + j].strip()
                    if potential_value and not potential_value.lower() in ['see title options', 'edit', '$', '', 'see pricing options', 'list faster', 'select all', 'lbs.', 'oz.', 'in. x in. x in.']:
                        if post_process:
                            return post_process(potential_value)
                        return potential_value
    return 'Unknown'

# File processing
if len(sys.argv) >= 3:
    files_to_process = [(os.path.normpath(sys.argv[1]), os.path.normpath(sys.argv[2]))]
else:
    html_candidates = sorted([f for f in os.listdir(script_dir) if f.lower().endswith(".html")])
    files_to_process = []
    for candidate in html_candidates:
        base = os.path.splitext(candidate)[0]
        txt_candidate = base + ".txt"
        html_path = os.path.join(script_dir, candidate)
        txt_path = os.path.join(script_dir, txt_candidate)
        if os.path.exists(txt_path):
            files_to_process.append((html_path, txt_path))
    if not files_to_process:
        print("No matching .html/.txt pairs found in the script directory – nothing to do.")
        sys.exit(1)

for html_file, text_file in files_to_process:
    # Extract item number from filename for standardized logging
    html_filename = os.path.basename(html_file)
    item_number = os.path.splitext(html_filename)[0]
    log_file = os.path.join(pull_logs_dir, f"{item_number}_pull_log.txt")
    log_message(f"Processing pair: HTML={html_file}, TXT={text_file} (Item: {item_number})")

    if not os.path.exists(html_file):
        log_message(f"HTML file does not exist: {html_file}")
        continue
    if not os.path.exists(text_file):
        log_message(f"Text file does not exist: {text_file}")
        continue
    log_message(f"HTML file exists: {html_file}")
    log_message(f"Text file exists: {text_file}")

    output_dir = os.path.dirname(os.path.abspath(html_file))
    if not os.access(output_dir, os.W_OK):
        log_message(f"Output directory is not writable: {output_dir}")
        continue
    log_message(f"Output directory is writable: {output_dir}")

    try:
        with open(html_file, "r", encoding="utf-8", errors="replace") as f:
            html_content = f.read().lstrip('\ufeff')
        log_message(f"Successfully read HTML file, length: {len(html_content)} characters")
    except Exception as e:
        log_message(f"Error reading HTML file: {str(e)} - {traceback.format_exc()}")
        continue

    try:
        with open(text_file, "r", encoding="utf-8", errors="replace") as f:
            text_content = f.read().lstrip('\ufeff')
        log_message(f"Successfully read text file, length: {len(text_content)} characters")
    except Exception as e:
        log_message(f"Error reading text file: {str(e)} - {traceback.format_exc()}")
        continue

    log_message(f"HTML content snippet (first 1000 chars): {html_content[:1000].replace('\n', ' ')}...")
    log_message(f"Text content snippet (first 1000 chars): {text_content[:1000].replace('\n', ' ')}...")

    try:
        log_message("Attempting to parse HTML with BeautifulSoup")
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            log_message("Parsed HTML with html.parser")
        except Exception as e:
            log_message(f"Failed to parse with html.parser: {e}, trying lxml")
            try:
                soup = BeautifulSoup(html_content, 'lxml')
                log_message("Parsed HTML with lxml")
            except ImportError:
                log_message("lxml parser not installed, sticking with html.parser")
                soup = BeautifulSoup(html_content, 'html.parser')
            except Exception as e:
                log_message(f"Failed to parse HTML: {str(e)} - {traceback.format_exc()}")
                continue

        if soup.html:
            log_message("Found <html> tag")
        else:
            log_message("No <html> tag found in content")
        if soup.body:
            log_message("Found <body> tag")
        else:
            log_message("No <body> tag found in content")
        if soup.find('h2', string=re.compile(r'Item specifics', re.I)):
            log_message("Found <h2>Item specifics</h2> tag")
        else:
            log_message("No <h2>Item specifics</h2> tag found")
        button_count = len(soup.find_all('button', attrs={'name': re.compile(r'attributes\..*')}))
        input_count = len(soup.find_all('input', attrs={'name': re.compile(r'universalProductCode|attributes\..*')}))
        log_message(f"Found {button_count} <button name='attributes.*'> tags")
        log_message(f"Found {input_count} <input name='universalProductCode|attributes.*'> tags")

        specifics_section = soup.find('div', class_=re.compile(r'smry.*summary__attributes'))
        if specifics_section:
            log_message("Found 'Item specifics' section with class match")
            log_message(f"Specifics section snippet: {str(specifics_section)[:1000].replace('\n', ' ')}...")
        else:
            log_message("No 'Item specifics' section found with class 'smry summary__attributes'")
            h2_specifics = soup.find('h2', string=re.compile(r'Item specifics', re.I))
            if h2_specifics:
                specifics_section = h2_specifics.find_parent('div')
                log_message("Found 'Item specifics' section via <h2> tag")
                log_message(f"Specifics section snippet: {str(specifics_section)[:1000].replace('\n', ' ')}...")
            else:
                log_message("No 'Item specifics' section found via <h2> tag")
                specifics_section = soup
                log_message("Falling back to entire HTML content")

        listing_details = {}
        field_count = 0
        # Updated: Broader class search for optional specifics
        fields = specifics_section.find_all('div', class_=re.compile(r'summary__attributes.*'))
        log_message(f"Number of potential field matches: {len(fields)}")

        for field in fields:
            field_count += 1
            log_message(f"Processing field #{field_count}")
            log_message(f"Field #{field_count} HTML snippet: {str(field)[:500].replace('\n', ' ')}...")

            key = None
            label_container = field.find('div', class_=re.compile(r'summary__attributes--label|se-field__label-container'))
            if label_container:
                for tag in [label_container.find('button', class_='fake-link tooltip__host'),
                            label_container.find('span'),
                            label_container.find('label')]:
                    if tag and tag.get_text(strip=True):
                        key = tag.get_text(strip=True)
                        break
                if key:
                    log_message(f"Extracted key: {key}")
                else:
                    log_message(f"No key found in field #{field_count}")
                    continue
            else:
                log_message(f"No label container found in field #{field_count}")
                continue

            value_container = field.find('div', class_=re.compile(r'summary__attributes--value|se-textbox--container'))
            value = ""
            if value_container:
                value_button = value_container.find('button', attrs={'name': re.compile(r'attributes\..*')})
                if value_button:
                    value_span = value_container.find('span', class_=re.compile(r'textual-display se-expand-button__button-text'))
                    if value_span and value_span.get_text(strip=True):
                        value = value_span.get_text(strip=True)
                        log_message(f"Found button value for key '{key}': {value}")

                if not value:
                    value_input = value_container.find('input', attrs={'name': re.compile(r'universalProductCode|attributes\..*')})
                    if value_input and value_input.get('value', '').strip():
                        value = value_input.get('value').strip()
                        log_message(f"Found input value for key '{key}': {value}")

                if not value:
                    log_message(f"No primary value found for key '{key}', skipping subtext")
            else:
                log_message(f"No value container found for key '{key}'")

            if value and value not in ['Enter number', 'Enter your own', '', 'See details']:
                listing_details[key] = value
                log_message(f"Added specific: {key}: {value}")
            else:
                log_message(f"Skipped placeholder or empty value for key '{key}': {value}")

        # Updated: Fallback to plaintext if no optional specifics found
        optional_keys = ['Model', 'Operating System', 'Type', 'GPU', 'SSD Capacity', 'Color', 'Maximum Resolution', 'Processor Speed', 'Series', 'RAM Size']
        if not any(key in optional_keys for key in listing_details):
            log_message("No optional specifics extracted from HTML - falling back to plaintext parsing")
            plaintext_specifics = extract_specifics_from_plaintext(text_content)
            listing_details.update(plaintext_specifics)

        seller_notes = 'Unknown'
        pattern = r'(?i)Seller Notes\s*[:\s]*([^\r\n]+)'
        match = re.search(pattern, text_content)
        if match:
            seller_notes = match.group(1).strip()
            if seller_notes not in ['Enter number', 'Enter your own', '']:
                listing_details['Seller Notes'] = seller_notes
                log_message(f"Extracted Seller Notes from plaintext: {seller_notes}")
        listing_details['Seller Notes'] = seller_notes
        log_message(f"Final Seller Notes: {seller_notes}")

        title_input = soup.find('input', attrs={'name': 'title'})
        listing_details['Title'] = title_input.get('value', 'Unknown').strip() if title_input else 'Unknown'
        if listing_details['Title'] == 'Unknown':
            listing_details['Title'] = extract_from_text(text_content, 'Item title', value_offset=1)
        log_message(f"Extracted Title: {listing_details['Title']}")

        sku_input = soup.find('input', attrs={'name': 'customLabel'})
        listing_details['Custom Label'] = sku_input.get('value', 'Unknown').strip() if sku_input else 'Unknown'
        if listing_details['Custom Label'] == 'Unknown':
            listing_details['Custom Label'] = extract_from_text(text_content, 'Custom label (SKU)', value_offset=1)
        log_message(f"Extracted Custom Label: {listing_details['Custom Label']}")

        category_info = extract_category_from_plaintext(text_content)
        listing_details['eBay Item Category'] = category_info
        log_message(f"Extracted eBay Item Category from plaintext: {listing_details['eBay Item Category']}")

        store_category = soup.find('button', attrs={'name': 'primaryStoreCategoryId'})
        listing_details['Store Category'] = store_category.find('span', class_='textual-display').get_text(strip=True) if store_category and store_category.find('span', class_='textual-display') else 'Unknown'
        if listing_details['Store Category'] == 'Unknown':
            listing_details['Store Category'] = extract_from_text(text_content, 'Store category', value_offset=1)
        log_message(f"Extracted Store Category: {listing_details['Store Category']}")

        store_subcategory = soup.find('button', attrs={'name': 'secondaryStoreCategoryId'})
        listing_details['Store Subcategory'] = store_subcategory.find('span', class_='textual-display').get_text(strip=True) if store_subcategory and store_subcategory.find('span', class_='textual-display') else 'Unknown'
        log_message(f"Extracted Store Subcategory: {listing_details['Store Subcategory']}")

        condition_button = soup.find('button', attrs={'name': 'condition'})
        listing_details['Condition'] = condition_button.get_text(strip=True) if condition_button else 'Unknown'
        if listing_details['Condition'] == 'Unknown':
            listing_details['Condition'] = extract_from_text(text_content, 'Item condition', value_offset=1)
        log_message(f"Extracted Condition: {listing_details['Condition']}")

        condition_desc = 'Unknown'
        if listing_details['Condition'].lower() != 'new':
            pattern = r'(?i)Condition description\s*([\s\S]*?)\s*\d+/\d+\s*$'
            match = re.search(pattern, text_content, re.MULTILINE)
            if match:
                condition_desc = match.group(1).strip()
                log_message(f"Extracted Condition Description from plaintext: {condition_desc}")
            else:
                condition_desc_tag = soup.find('textarea', attrs={'name': 'itemConditionDescription'})
                condition_desc = condition_desc_tag.get_text(strip=True) if condition_desc_tag else 'Unknown'
                log_message(f"Fallback to HTML for Condition Description: {condition_desc}")
        else:
            condition_desc = 'N/A'
        listing_details['Condition Description'] = condition_desc
        log_message(f"Final Condition Description: {listing_details['Condition Description']}")

        shipping_policy = soup.find('input', attrs={'name': 'shippingPolicyId'})
        listing_details['Shipping Policy'] = shipping_policy.get('value', 'Unknown').strip() if shipping_policy else 'Unknown'
        if listing_details['Shipping Policy'] == 'Unknown':
            listing_details['Shipping Policy'] = extract_from_text(text_content, 'Shipping policy', value_offset=1)
        log_message(f"Extracted Shipping Policy: {listing_details['Shipping Policy']}")

        return_policy = 'Unknown'
        pattern = r'(?i)Return policy[:\s]*([^\r\n]+)'
        match = re.search(pattern, text_content)
        if match:
            return_policy = match.group(1).strip()
            log_message(f"Extracted Return Policy from plaintext: {return_policy}")
        else:
            return_policy_div = soup.find('div', string=re.compile(r'Return policy', re.I))
            if return_policy_div:
                next_span = return_policy_div.find_next('span', class_='textual-display')
                return_policy = next_span.get_text(strip=True) if next_span else 'Unknown'
                log_message(f"Fallback to HTML for Return Policy: {return_policy}")

        # EXTRA FALLBACK: Reconstruct return policy from nearby plaintext context when still Unknown
        if return_policy == 'Unknown':
            try:
                lines = [ln.strip() for ln in text_content.split('\n')]
                # Try to scope to the block after the 'Return policy' label if present
                start_idx = next((i for i, ln in enumerate(lines) if re.search(r'(?i)^Return\s+policy', ln)), -1)
                window = lines[start_idx:start_idx + 20] if start_idx >= 0 else lines[:]

                block_text = '\n'.join(window)
                # Quick checks
                if re.search(r'(?i)no\s+returns\s+accepted', block_text):
                    return_policy = 'Returns Not Accepted'
                else:
                    accepted = bool(re.search(r'(?i)returns?\s+accepted', block_text) or re.search(r'(?i)accepted\s+within\s+\d+\s*days?', block_text))
                    days_match = re.search(r'(?i)accepted\s+within\s+(\d{1,3})\s*days?', block_text)
                    days = days_match.group(1) if days_match else None

                    # Payer detection (default Buyer if unclear)
                    payer = 'Buyer'
                    if re.search(r'(?i)seller\s+pays\s+return\s+shipping', block_text):
                        payer = 'Seller'
                    elif re.search(r'(?i)buyer\s+pays\s+return\s+shipping', block_text):
                        payer = 'Buyer'

                    # Method detection
                    method = None
                    if re.search(r'(?i)money\s*back\s*or\s*replacement', block_text):
                        method = 'Money Back or Replacement'
                    elif re.search(r'(?i)money\s*back', block_text):
                        method = 'Money Back'
                    elif re.search(r'(?i)exchange', block_text):
                        method = 'Exchange'

                    if accepted and days:
                        if not method:
                            method = 'Money Back'
                        return_policy = f"Returns Accepted,{payer},{int(days)} Days,{method}#0"
                        log_message(f"RETURN_POLICY_FALLBACK constructed: {return_policy}")
                    elif accepted:
                        # Accepted but window missing; provide best-effort
                        if not method:
                            method = 'Money Back'
                        return_policy = f"Returns Accepted,{payer},30 Days,{method}#0"
                        log_message("RETURN_POLICY_FALLBACK accepted-without-window; defaulting to 30 Days")
                    # else keep Unknown
            except Exception as e:
                log_message(f"Return policy extra fallback error: {e}")

        listing_details['Return Policy'] = return_policy
        log_message(f"Final Return Policy: {return_policy}")

        quantity_input = soup.find('input', attrs={'name': 'quantity'})
        listing_details['Quantity'] = quantity_input.get('value', 'Unknown').strip() if quantity_input else 'Unknown'
        if listing_details['Quantity'] == 'Unknown':
            listing_details['Quantity'] = extract_from_text(text_content, 'Quantity', value_offset=1)
        log_message(f"Extracted Quantity: {listing_details['Quantity']}")

        lot_input = soup.find('input', attrs={'name': 'lotSize'})
        listing_details['Lot Amount'] = lot_input.get('value', 'Unknown').strip() if lot_input else 'Unknown'
        if listing_details['Lot Amount'] == 'Unknown':
            listing_details['Lot Amount'] = extract_from_text(text_content, 'Quantity in lot', value_offset=1)
        log_message(f"Extracted Lot Amount: {listing_details['Lot Amount']}")

        offers_input = soup.find('input', attrs={'name': 'bestOfferEnabled'})
        listing_details['Offers Enabled'] = 'Yes' if offers_input and offers_input.get('checked') else 'No'
        if listing_details['Offers Enabled'] == 'No':
            offers_text = extract_from_text(text_content, 'Allow offers', value_offset=1)
            if offers_text != 'Unknown':
                listing_details['Offers Enabled'] = 'Yes'
        log_message(f"Extracted Offers Enabled: {listing_details['Offers Enabled']}")

        minimum_offer = 'Unknown'
        if listing_details['Offers Enabled'] == 'Yes':
            min_offer_input = soup.find('input', attrs={'name': 'minimumBestOffer'})
            minimum_offer = min_offer_input.get('value', 'Unknown').strip() if min_offer_input else 'Unknown'
        listing_details['Minimum Offer'] = minimum_offer
        log_message(f"Extracted Minimum Offer: {minimum_offer}")

        auto_accept = 'Unknown'
        if listing_details['Offers Enabled'] == 'Yes':
            auto_accept_input = soup.find('input', attrs={'name': 'autoAcceptPrice'})
            auto_accept = auto_accept_input.get('value', 'Unknown').strip() if auto_accept_input else 'Unknown'
        listing_details['Auto-Accept'] = auto_accept
        log_message(f"Extracted Auto-Accept: {auto_accept}")

        current_price = 'Unknown'
        start_price = soup.find('input', attrs={'name': 'startPrice'})
        bin_price = soup.find('input', attrs={'name': 'binPrice'})
        if start_price and start_price.get('value'):
            current_price = start_price.get('value').strip()
        elif bin_price and bin_price.get('value'):
            current_price = bin_price.get('value').strip()
        if current_price == 'Unknown':
            current_price = extract_from_text(text_content, 'Starting bid', value_offset=1, post_process=lambda v: v.replace('$', '').strip() if '$' in v else v)
        listing_details['Current Price'] = current_price
        log_message(f"Extracted Current Price: {current_price}")

        buy_it_now_price = 'Unknown'
        if bin_price and bin_price.get('value'):
            buy_it_now_price = bin_price.get('value').strip()
        listing_details['Buy It Now Price'] = buy_it_now_price
        log_message(f"Extracted Buy It Now Price: {buy_it_now_price}")

        format_select = soup.find('select', attrs={'name': 'format'})
        listing_type_raw = 'Unknown'
        if format_select:
            selected_option = format_select.find('option', attrs={'selected': ''})
            listing_type_raw = selected_option.get('value', 'Unknown').strip() if selected_option else 'Unknown'

        if listing_type_raw == 'Unknown':
            format_line = extract_from_text(text_content, 'Format', value_offset=1)
            if 'Auction' in format_line:
                listing_type_raw = 'ChineseAuction'
            elif 'Buy It Now' in format_line:
                listing_type_raw = 'FixedPrice'

        if listing_type_raw == 'ChineseAuction':
            listing_type = 'Auction'
        elif listing_type_raw == 'FixedPrice' or buy_it_now_price != 'Unknown':
            listing_type = 'BuyItNow'
        else:
            listing_type = 'Unknown'
        listing_details['Listing Type'] = listing_type
        log_message(f"Extracted Listing Type: {listing_details['Listing Type']}")

        scheduled_date = 'Unknown'
        scheduled_time = 'Unknown'
        timezone = 'Unknown'
        is_scheduled = False

        schedule_input = soup.find('input', attrs={'name': 'scheduleListingSelection'})
        if schedule_input and schedule_input.get('checked'):
            is_scheduled = True
            start_date = soup.find('input', attrs={'name': 'startDate'})
            start_time = soup.find('input', attrs={'name': 'startTime'})
            start_timezone = soup.find('input', attrs={'name': 'startTimezone'})
            if start_date:
                scheduled_date = start_date.get('value', 'Unknown').strip()
            if start_time:
                scheduled_time = start_time.get('value', 'Unknown').strip()
            if start_timezone:
                timezone = start_timezone.get('value', 'Unknown').strip()

        schedule_date_input = soup.find('input', attrs={'name': 'scheduleStartDate'})
        if schedule_date_input and schedule_date_input.get('value'):
            is_scheduled = True
            scheduled_date = schedule_date_input.get('value', 'Unknown').strip()
            log_message(f"Found scheduleStartDate: {scheduled_date}")

            hours_select = soup.find('select', attrs={'name': 'localizedStartHours'})
            if hours_select:
                selected_hour = hours_select.find('option', attrs={'selected': ''})
                if selected_hour:
                    hour_value = selected_hour.get('value', '12')
                    log_message(f"Found selected hour: {hour_value}")
            else:
                hour_value = '12'

            minutes_select = soup.find('select', attrs={'name': 'localizedStartMinutes'})
            if minutes_select:
                selected_minute = minutes_select.find('option', attrs={'selected': ''})
                if selected_minute:
                    minute_value = selected_minute.get('value', '00')
                    log_message(f"Found selected minute: {minute_value}")
            else:
                minute_value = '00'

            meridian_select = soup.find('select', attrs={'name': 'meridian'})
            if meridian_select:
                selected_meridian = meridian_select.find('option', attrs={'selected': ''})
                if selected_meridian:
                    meridian_value = selected_meridian.get('value', 'AM')
                    log_message(f"Found selected meridian: {meridian_value}")
            else:
                meridian_value = 'AM'

            scheduled_time = f"{hour_value}:{minute_value} {meridian_value}"
            log_message(f"Constructed scheduled time: {scheduled_time}")

        if timezone == 'Unknown':
            timezone_patterns = [
                r'time-zone.*?>(PDT|PST|EDT|EST|CDT|CST|MDT|MST)<',
                r'>(PDT|PST|EDT|EST|CDT|CST|MDT|MST)</div>',
                r'>(PDT|PST|EDT|EST|CDT|CST|MDT|MST)</span>'
            ]
            for pattern in timezone_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE)
                if match:
                    timezone = match.group(1).upper()
                    log_message(f"Extracted timezone from HTML: {timezone}")
                    break

        # Plaintext fallbacks: only match explicit labels; avoid generic words like 'Days'
        if scheduled_date == 'Unknown' or timezone == 'Unknown' or scheduled_time in ('Unknown', 'Not Scheduled'):
            # Fallback DATE extraction: "Start Date:" or "Scheduled Start Date:" or "Scheduled Date:"
            if scheduled_date == 'Unknown':
                date_patterns = [
                    r'(?im)^Start\s*Date\s*[:\s]+([^\r\n]+)\s*$',
                    r'(?im)^Scheduled\s*(?:Start\s*)?Date\s*[:\s]+([^\r\n]+)\s*$'
                ]
                for pattern in date_patterns:
                    match = re.search(pattern, text_content)
                    if match:
                        candidate = match.group(1).strip()
                        # Guard out obvious non-dates like 's' or 'Days'
                        if candidate.lower() not in {'s', 'days'}:
                            scheduled_date = candidate
                            is_scheduled = True
                            log_message(f"Extracted Scheduled Date from plaintext (label match): {scheduled_date}")
                            break

            # Fallback TIME extraction: "Start Time:" or "Scheduled Start Time:" with HH:MM AM/PM
            if scheduled_time in ('Unknown', 'Not Scheduled'):
                time_patterns = [
                    r'(?im)^Start\s*Time\s*[:\s]+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM))\s*$',
                    r'(?im)^Scheduled\s*(?:Start\s*)?Time\s*[:\s]+([0-9]{1,2}:[0-9]{2}\s*(?:AM|PM))\s*$'
                ]
                for pattern in time_patterns:
                    match = re.search(pattern, text_content)
                    if match:
                        scheduled_time = match.group(1).strip().upper()
                        is_scheduled = True
                        log_message(f"Extracted Scheduled Time from plaintext (label match): {scheduled_time}")
                        break

            # Fallback TIMEZONE extraction at end of line
            if timezone == 'Unknown':
                tz_match = re.search(r'(?im)(PDT|PST|EDT|EST|CDT|CST|MDT|MST)\s*$', text_content)
                if tz_match:
                    timezone = tz_match.group(1).strip().upper()
                    log_message(f"Extracted Timezone from plaintext: {timezone}")

        listing_details['Scheduled Date'] = scheduled_date if is_scheduled else 'Unknown'
        listing_details['Scheduled Time'] = scheduled_time if is_scheduled and scheduled_time != 'Unknown' else 'Not Scheduled'
        listing_details['Timezone'] = timezone if is_scheduled else 'Unknown'

        log_message(f"Final Scheduled Date: {listing_details['Scheduled Date']}")
        log_message(f"Final Scheduled Time: {listing_details['Scheduled Time']}")
        log_message(f"Final Timezone: {listing_details['Timezone']}")
        log_message(f"Is Scheduled: {is_scheduled}")

        major_input = soup.find('input', attrs={'name': 'majorWeight'})
        minor_input = soup.find('input', attrs={'name': 'minorWeight'})
        major_val = major_input.get('value', '').strip() if major_input else ''
        minor_val = minor_input.get('value', '').strip() if minor_input else ''
        weight = 'Unknown'
        if major_val or minor_val:
            parts = []
            if major_val:
                parts.append(f"{major_val} lbs.")
            if minor_val and minor_val not in ['oz.', '']:
                parts.append(f"{minor_val} oz.")
            weight = ' '.join(parts)
        if weight == 'Unknown':
            weight_line = extract_from_text(text_content, 'Package weight', value_offset=2)
            if weight_line != 'Unknown' and weight_line not in ['oz.', '']:
                weight = f"{weight_line} lbs."
        listing_details['Package Weight'] = weight
        log_message(f"Extracted package weight: {weight}")

        len_input = soup.find('input', attrs={'name': 'packageLength'})
        wid_input = soup.find('input', attrs={'name': 'packageWidth'})
        dep_input = soup.find('input', attrs={'name': 'packageDepth'})
        l_val = len_input.get('value', '').strip() if len_input else ''
        w_val = wid_input.get('value', '').strip() if wid_input else ''
        d_val = dep_input.get('value', '').strip() if dep_input else ''
        dimensions = 'Unknown'
        if l_val or w_val or d_val:
            l = l_val or '0'
            w = w_val or '0'
            d = d_val or '0'
            dimensions = f"{l}x{w}x{d} in."
        if dimensions == 'Unknown':
            dim_line = extract_from_text(text_content, 'Package dimensions', value_offset=1)
            if dim_line != 'Unknown' and dim_line != 'in. x in. x in.':
                dimensions = dim_line
        listing_details['Package Dimensions'] = dimensions
        log_message(f"Extracted package dimensions: {dimensions}")

        # Classify listing location. Treat as Scheduled if we detected scheduling at all,
        # even when only partial info (e.g., missing time) was extracted.
        listing_location = 'Active'
        if (
            is_scheduled
            or (
                listing_details.get('Scheduled Time', 'Not Scheduled') != 'Not Scheduled'
                and listing_details.get('Scheduled Date', 'Unknown') != 'Unknown'
            )
        ):
            listing_location = 'Scheduled'
        listing_details['Listing Location'] = listing_location
        log_message(f"Determined Listing Location: {listing_location}")

        description_file = os.path.normpath(html_file.replace('.html', '_description.txt'))
        html_filename = os.path.basename(html_file)
        item_number = os.path.splitext(html_filename)[0]
        log_message(f"Extracted item number from filename: {item_number}")

        lot_amount = listing_details.get('Lot Amount', '1')
        try:
            lot_int = int(lot_amount)
            lot_info = 'Single item' if lot_int == 1 else f"{lot_int} items per lot"
        except ValueError:
            lot_info = 'Single item' if lot_amount in ('1', 'Unknown') else f"{lot_amount} items per lot"

        header_info = (
            f"Title: {listing_details.get('Title', 'Unknown')}\n"
            f"Custom Label: {listing_details.get('Custom Label', 'Unknown')}\n"
            f"Listing Info: {lot_info}\n"
            f"Item Number: {item_number}\n\n"
        )

        meta_map = {
            'meta_listing_location_key': listing_details.get('Listing Location', 'Unknown'),
            'meta_listing_type_key': listing_details.get('Listing Type', 'Unknown'),
            'meta_listing_shippingpolicy_key': listing_details.get('Shipping Policy', 'Unknown'),
            'meta_listing_returnpolicy_key': listing_details.get('Return Policy', 'Unknown'),
            'meta_listing_condition_key': listing_details.get('Condition', 'Unknown'),
            'meta_listing_conditiondescription_key': listing_details.get('Condition Description', 'Unknown'),
            'meta_listing_storecategory_key': listing_details.get('Store Category', 'Unknown'),
            'meta_listing_storesubcategory_key': listing_details.get('Store Subcategory', 'Unknown'),
            'meta_listing_price_key': listing_details.get('Current Price', 'Unknown'),
            'meta_listing_offers_enabled_key': listing_details.get('Offers Enabled', 'Unknown'),
            'meta_listing_scheduled_date_key': listing_details.get('Scheduled Date', 'Unknown'),
            'meta_listing_scheduled_time_key': listing_details.get('Scheduled Time', 'Unknown'),
            'meta_listing_scheduled_timezone_key': listing_details.get('Timezone', 'Unknown'),
            'meta_listing_package_weight_key': listing_details.get('Package Weight', 'Unknown'),
            'meta_listing_package_dimensions_key': listing_details.get('Package Dimensions', 'Unknown')
        }

        if listing_details.get('Buy It Now Price', 'Unknown') != 'Unknown':
            meta_map['meta_listing_buyitnow_price_key'] = listing_details['Buy It Now Price']

        ordered_meta_keys = [
            'meta_listing_location_key',
            'meta_listing_type_key',
            '',
            'meta_listing_shippingpolicy_key',
            'meta_listing_returnpolicy_key',
            'meta_listing_condition_key',
            'meta_listing_conditiondescription_key',
            'meta_listing_storecategory_key',
            'meta_listing_storesubcategory_key',
            'meta_listing_price_key',
            'meta_listing_buyitnow_price_key',
            'meta_listing_offers_enabled_key',
            'meta_listing_scheduled_date_key',
            'meta_listing_scheduled_time_key',
            'meta_listing_scheduled_timezone_key',
            'meta_listing_package_weight_key',
            'meta_listing_package_dimensions_key'
        ]

        metadata_lines = []
        for key in ordered_meta_keys:
            if key == '':
                metadata_lines.append('')
                continue
            if key in meta_map:
                value = meta_map[key] if meta_map[key] else 'Unknown'
                if key == 'meta_listing_storesubcategory_key' and value == 'Unknown':
                    continue
                metadata_lines.append(f"{key}: {value}")
        metadata_section = "\n".join(metadata_lines)

        category_path_raw = listing_details.get('eBay Item Category', '')
        category_lines = []
        if category_path_raw and ' > ' in category_path_raw:
            levels = [c.strip() for c in category_path_raw.split(' > ') if c.strip()]
            category_lines.extend(levels)
        else:
            category_lines.append('Unknown Category')
        category_section = "\n".join(category_lines)

        exclude_keys = {
            'Title', 'Custom Label', 'Listing Status', 'Listing Type', 'Current Price',
            'Buy It Now Price', 'Lot Amount', 'Quantity', 'Minimum Offer',
            'Auto-Accept', 'Scheduled Date', 'Scheduled Time', 'Timezone',
            'Package Weight', 'Package Dimensions', 'Store Category', 'Store Subcategory',
            'Shipping Policy', 'Return Policy', 'eBay Item Category', 'Condition Description',
            'Offers Enabled', 'Listing Location', 'Condition', 'Seller Notes'
        }

        specifics_lines = []
        if listing_details.get('Seller Notes') and listing_details['Seller Notes'] != 'Unknown':
            specifics_lines.append(f"Seller Notes: {listing_details['Seller Notes']}")

        remaining = [
            (k, v) for k, v in listing_details.items()
            if k not in exclude_keys and v and v != 'Unknown'
        ]
        for k, v in sorted(remaining):
            specifics_lines.append(f"{k}: {v}")

        if not specifics_lines:
            specifics_lines.append('No specifics found')

        specifics_section = "\n".join(specifics_lines)

        content = (
            header_info +
            "=== METADATA ===\n" +
            metadata_section + "\n\n" +
            "===CATEGORY PATH===\n" +
            category_section + "\n\n" +
            "===ITEM SPECIFICS===\n" +
            specifics_section + "\n\n" +
            "=== TABLE DATA ===\n\n" +
            "=== ITEM DESCRIPTION ===\n\n"
        )

        try:
            with open(description_file, 'w', encoding='utf-8') as f:
                f.write(content)
            log_message(f"Saved description to: {description_file}")
        except Exception as e:
            log_message(f"Error saving description file: {str(e)} - {traceback.format_exc()}")

        log_message(
            f"Processed {len(fields)} fields, extracted {len(listing_details)} listing details"
        )

    except Exception as e:
        log_message(f"Error during extraction: {str(e)} - {traceback.format_exc()}")