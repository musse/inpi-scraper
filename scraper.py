import argparse
import browser_cookie3
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import random
import os
import webbrowser
from datetime import datetime
import json
import hashlib
import sys

# add your cookie string here or use browser_cookie3
COOKIES_STRING = ""


class INPIPatentScraper:
    def __init__(self, csv_file, state_file, cookies=None, debug=False, use_browser_cookies=True):
        self.base_url = "https://busca.inpi.gov.br/pePI/servlet/PatenteServletController"
        self.login_url = "https://busca.inpi.gov.br/pePI/servlet/LoginController"
        self.session = requests.Session()
        self.debug = debug
        self.csv_file = csv_file
        self.state_file = state_file

        # Headers to mimic a browser request
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pt-BR,en-US;q=0.8,en;q=0.6,fr-FR;q=0.4,fr;q=0.2',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://busca.inpi.gov.br/pePI/jsp/patentes/PatenteSearchBasico.jsp',
            'Content-Type': 'application/x-www-form-urlencoded',
        }

        if use_browser_cookies:
            try:
                cj = browser_cookie3.firefox(domain_name='inpi.gov.br')
                self.session.cookies.update(cj)
                print(f"Loaded cookies from Firefox browser for inpi.gov.br")
            except Exception as e:
                print(f"Error loading browser cookies: {e}")

        # Add cookies if provided
        # if cookies:
        #     if isinstance(cookies, str):
        #         # If cookies are provided as a string
        #         self.session.headers.update({'Cookie': cookies})
        #     elif isinstance(cookies, dict):
        #         # If cookies are provided as a dictionary
        #         for name, value in cookies.items():
        #             self.session.cookies.set(name, value)

        # self.session.headers.update(self.headers)

        # Session status
        self.authenticated = False
        self.session_expired = False

        # Initialize storage for scraped data
        self.patents = []
        self.detailed_patents = []

        # Track already processed patents
        self.processed_patent_ids = set()

        # Search state
        self.search_state = {
            'last_query': None,
            'last_search_column': None,
            'last_page_processed': 0,
            'total_pages': 0,
            'has_more_pages': True,
            'found_patents': {},  # Store all found patents by ID
            'last_update_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def load_existing_data(self, csv_filename, state_filename):
        """
        Load existing data from CSV and search state from JSON file

        Args:
            csv_filename (str): Name of the CSV file to load
            state_filename (str): Name of the JSON file containing search state

        Returns:
            set: Set of patent IDs that have already been processed
        """
        # Load processed patents from CSV
        try:
            if os.path.exists(csv_filename):
                df = pd.read_csv(csv_filename)
                if 'patent_id' in df.columns:
                    processed_ids = set(df['patent_id'].astype(str).tolist())
                    print(f"Loaded {len(processed_ids)} processed patent IDs from {csv_filename}")
                    self.processed_patent_ids = processed_ids

                    # Create a dictionary to track which patents have details in the CSV
                    self.csv_patents_dict = {}
                    for _, row in df.iterrows():
                        patent_id = str(row['patent_id'])
                        self.csv_patents_dict[patent_id] = {
                            'patent_number': row.get('patent_number', ''),
                            'has_details': 'patent_agent' in df.columns and not pd.isna(row.get('patent_agent', '')),
                            'row': row.to_dict()
                        }

                    print(f"Created reference dictionary with {len(self.csv_patents_dict)} patents")
                else:
                    print(f"No 'patent_id' column found in {csv_filename}")
            else:
                print(f"File {csv_filename} does not exist yet")
                self.csv_patents_dict = {}
        except Exception as e:
            print(f"Error loading existing data from CSV: {e}")
            self.csv_patents_dict = {}

        # Load search state from JSON file
        try:
            if os.path.exists(state_filename):
                with open(state_filename, 'r', encoding='utf-8') as f:
                    self.search_state = json.load(f)
                print(f"Loaded search state from {state_filename}")
                print(f"  Last query: {self.search_state['last_query']}")
                print(f"  Last page processed: {self.search_state['last_page_processed']}")
                print(f"  Total pages: {self.search_state['total_pages']}")
                print(f"  Has more pages: {self.search_state['has_more_pages']}")
                print(f"  Found patents: {len(self.search_state['found_patents'])}")

                # Import found patents into our patents list
                # But skip ones we've already processed
                patents_to_process = []
                for patent_id, patent_data in self.search_state['found_patents'].items():
                    if patent_id not in self.processed_patent_ids:
                        # Check if it has details in the CSV
                        has_details = patent_id in self.csv_patents_dict and self.csv_patents_dict[patent_id].get('has_details', False)
                        if not has_details:
                            patents_to_process.append(patent_data)

                if patents_to_process:
                    self.patents.extend(patents_to_process)
                    print(f"Imported {len(patents_to_process)} patents from search state for processing")
                else:
                    print("No patents from search state need processing")
        except Exception as e:
            print(f"Error loading search state: {e}")

        return self.processed_patent_ids

    def save_search_state(self):
        """
        Save the current search state to a JSON file

        Args:
            filename (str): Name of the JSON file to save to
        """
        filename = self.state_file
        try:
            # Update last update time
            self.search_state['last_update_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.search_state, f, ensure_ascii=False, indent=2)
            print(f"Saved search state to {filename}")
        except Exception as e:
            print(f"Error saving search state: {e}")

    def check_and_renew_session(self):
        """
        Check if the session has expired and renew it if necessary

        Returns:
            bool: True if session is valid or was successfully renewed, False otherwise
        """
        if self.session_expired or not self.is_authenticated():
            return False
        return True

    def search(self, query, search_column, max_pages=None, continue_from_last=True):
        """
        Perform a search for patents with the given query

        Args:
            query (str): Search query (e.g. "petroleo brasileiro")
            search_column (str): Column to search in (e.g. "NomeDepositante", "Titulo", etc.)
            max_pages (int, optional): Maximum number of pages to scrape. If None, scrape all pages.
            continue_from_last (bool): Whether to continue from the last page processed

        Returns:
            DataFrame: Pandas DataFrame containing all scraped patent information
        """
        # Check if session is valid
        if not self.check_and_renew_session():
            return None

        # Check if we should continue a previous search
        start_page = 1
        if continue_from_last and self.search_state['last_query'] == query and self.search_state['last_search_column'] == search_column:
            # Continue from previous search regardless of has_more_pages
            start_page = self.search_state['last_page_processed'] + 1
            print(f"Continuing search from page {start_page}")

            # If has_more_pages is False, we've already processed all pages
            if not self.search_state['has_more_pages']:
                print("All pages have already been processed. Skipping search query.")
                return pd.DataFrame(self.patents)
        else:
            # Reset search state for new search
            self.search_state = {
                'last_query': query,
                'last_search_column': search_column,
                'last_page_processed': 0,
                'total_pages': 0,
                'has_more_pages': True,
                'found_patents': self.search_state.get('found_patents', {}),  # Keep existing patents
                'last_update_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            print(f"Starting new search for: {query} in column: {search_column}")

        # If we're starting a new search or starting from page 1
        if start_page == 1:
            # Initial search form data for POST request
            # Convert query to ISO-8859-1 encoding which appears to be used by the site
            encoded_query = query
            try:
                # Try to encode as ISO-8859-1 if it's a standard string
                if isinstance(query, str):
                    encoded_query = query.encode('iso-8859-1').decode('iso-8859-1')
            except:
                # If encoding fails, just use the original query
                pass

            form_data = {
                'NumPedido': '',
                'NumGru': '',
                'NumProtocolo': '',
                'FormaPesquisa': 'todasPalavras',
                'ExpressaoPesquisa': encoded_query,
                'Coluna': search_column,
                'RegisterPerPage': '100',  # Increased to 100 results per page
                'botao': ' pesquisar » ',
                'Action': 'SearchBasico'
            }

            # Make the POST request
            response = self.session.post(
                self.base_url,
                data=form_data,
                allow_redirects=True
            )

            if response.status_code != 200:
                print(f"Failed to perform search: {response.status_code}")
                print(response.text[:500])  # Print first 500 chars to help debug
                return None

            # Check if we got a login page instead of search results
            if self.is_login_page(response.text):
                print("Session expired during search.")
                return None

            # Store the page content
            page_content = response.text

            # Debug mode - open in browser if enabled
            if self.debug:
                self._debug_response(response, "search_results")

            # Parse first page
            self._parse_page(page_content)

            # Update search state - first page is processed
            self.search_state['last_page_processed'] = 1

            # Save the page content as HTML
            self._save_page_content(page_content, page=1)

            # Get total number of pages
            soup = BeautifulSoup(page_content, 'html.parser')
            pagination_text = soup.select("font.normal")

            total_pages = 1
            for text in pagination_text:
                match = re.search(r'Mostrando página \<b\>(\d+)\<\/b\> de \<b\>(\d+)\<\/b\>', str(text))
                if match:
                    total_pages = int(match.group(2))
                    print(f"Found {total_pages} pages of results")
                    self.search_state['total_pages'] = total_pages
                    break
        else:
            # We're continuing from a previous search
            total_pages = self.search_state['total_pages']

        # Set max_pages if not specified
        if max_pages is None:
            max_pages = total_pages
        else:
            max_pages = min(max_pages, total_pages)

        # Scrape remaining pages
        for page in range(start_page, max_pages + 1):
            if page > 1:  # Skip page 1 if we're starting a new search (already processed above)
                print(f"Scraping page {page} of {max_pages}")

                # For subsequent pages, we use the nextPage action with GET
                next_params = {
                    'Action': 'nextPage',
                    'Page': page,
                    'Resumo': '',
                    'Titulo': ''
                }

                # Add a delay to be polite to the server
                time.sleep(1.0)  # random.uniform(1.0, 3.0))

                # Check if session is still valid
                if not self.check_and_renew_session():
                    print("Failed to maintain session. Saving progress and exiting.")
                    self.search_state['last_page_processed'] = page - 1
                    self.search_state['has_more_pages'] = True
                    self.save_search_state()
                    break

                response = self.session.get(
                    self.base_url,
                    params=next_params
                )

                if response.status_code != 200:
                    print(f"Failed to retrieve page {page}: {response.status_code}")
                    # Update search state to indicate where we stopped
                    self.search_state['last_page_processed'] = page - 1
                    self.search_state['has_more_pages'] = True
                    self.save_search_state()
                    break

                # Check if we got a login page
                if self.is_login_page(response.text):
                    print(f"Session expired while retrieving page {page}.")
                    self.search_state['last_page_processed'] = page - 1
                    self.search_state['has_more_pages'] = True
                    self.save_search_state()
                    break

                # Store the page content
                page_content = response.text

                # Save the page content as HTML
                self._save_page_content(page_content, page=page)

                # Parse the page
                self._parse_page(page_content)

                # Update search state after each page
                self.search_state['last_page_processed'] = page

            # Save search state periodically (every 5 pages)
            if page % 5 == 0 or page == max_pages:
                self.save_search_state()

        # Update search state after completing all pages
        self.search_state['has_more_pages'] = (self.search_state['last_page_processed'] < total_pages)
        self.save_search_state()

        # Filter out already processed patents that have details
        if self.processed_patent_ids:
            original_count = len(self.patents)
            self.patents = [p for p in self.patents if p['patent_id'] not in self.processed_patent_ids or
                            (p['patent_id'] in self.csv_patents_dict and not self.csv_patents_dict[p['patent_id']].get('has_details', False))]
            print(f"Filtered out {original_count - len(self.patents)} already processed patents with details")

        # Convert to DataFrame
        return pd.DataFrame(self.patents)

    def is_login_page(self, html_content):
        """
        Check if the HTML content is a login page

        Args:
            html_content (str): HTML content to check

        Returns:
            bool: True if the HTML is a login page, False otherwise
        """
        # Look for indicators of a login page
        login_indicators = [
            'pePI - Pesquisa em Propriedade Industrial',
            'Entrar com GOV.BR',
            'Para realizar a Pesquisa anonimamente',
            'name="T_Login"',
            'name="T_Senha"'
        ]

        # Check for the presence of login indicators
        for indicator in login_indicators:
            if indicator in html_content:
                return True

        return False

    def _save_page_content(self, html_content, page=1):
        """
        Save the HTML content to a cache folder

        Args:
            html_content (str): The HTML content to save
            page (int): The page number
        """
        # Create cache directory if it doesn't exist
        cache_dir = "inpi_cache"
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        # Generate a filename based on query, page, and date
        query = self.search_state['last_query'] or "unknown"
        query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
        filename = f"{cache_dir}/search_{query_hash}_page_{page}.html"

        # Save the HTML content
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)

    def _remove_line_breaks(self, text):
        """
        Remove line breaks and extra whitespace from text

        Args:
            text: The text to clean

        Returns:
            str: Cleaned text
        """
        if text is None:
            return None

        if isinstance(text, list):
            return [self._remove_line_breaks(item) for item in text]

        if isinstance(text, str):
            # Replace line breaks and multiple spaces with a single space
            cleaned = re.sub(r'\s+', ' ', text)
            return cleaned.strip()

        return text

    def _parse_page(self, html_content):
        """
        Parse the HTML content of a page and extract patent information

        Args:
            html_content (str): HTML content of the page
        """
        soup = BeautifulSoup(html_content, 'html.parser')

        # Find the table containing the patent rows
        table_rows = soup.select("tbody#tituloContext tr")

        for row in table_rows:
            try:
                # Extract the patent data
                patent_number = row.select_one("td:nth-of-type(2) a").text.strip()
                filing_date = row.select_one("td:nth-of-type(3) font").text.strip()

                # Get the patent ID for later use in fetching details
                patent_link = row.select_one("td:nth-of-type(2) a")['href']
                patent_id_match = re.search(r'CodPedido=(\d+)', patent_link)
                patent_id = patent_id_match.group(1) if patent_id_match else None

                # Skip if no patent ID found
                if not patent_id:
                    continue

                # Extract search parameters from the URL for detail page access
                search_param_match = re.search(r'SearchParameter=([^&]+)', patent_link)
                search_param = search_param_match.group(1) if search_param_match else ''

                # Try to extract title if present
                title_cell = row.select_one("td:nth-of-type(4) font b")
                title = title_cell.text.strip() if title_cell and title_cell.text.strip() else None
                title = self._remove_line_breaks(title)

                # Try to extract IPC if present
                ipc_cell = row.select_one("td:nth-of-type(5) font")
                ipc = ipc_cell.text.strip() if ipc_cell and ipc_cell.text.strip() != '-' else None
                ipc = self._remove_line_breaks(ipc)

                # Extract the patent_number_raw (for creating detail URLs)
                patent_number_raw = re.sub(r'[^\d]', '', patent_number)

                # Create patent data dictionary
                patent_data = {
                    'patent_number': patent_number,
                    'filing_date': filing_date,
                    'patent_id': patent_id,
                    'title': title,
                    'ipc': ipc,
                    'patent_number_raw': patent_number_raw,
                    'search_param': search_param
                }

                # Store in search state
                self.search_state['found_patents'][patent_id] = patent_data

                # Check if we should process this patent
                # We process if:
                # 1. It's not in the processed list, OR
                # 2. It's in the CSV but doesn't have details
                should_process = (
                    patent_id not in self.processed_patent_ids or
                    (patent_id in self.csv_patents_dict and not self.csv_patents_dict[patent_id].get('has_details', False))
                )

                if should_process:
                    # Add to our list for further processing
                    self.patents.append(patent_data)

            except Exception as e:
                print(f"Error parsing row: {e}")

    def get_patent_details(self, patent_id, search_param='', resumo='', titulo=''):
        """
        Get the details for a specific patent

        Args:
            patent_id (str): The ID of the patent to get details for
            search_param (str): Search parameter from the original search
            resumo (str): Resumo parameter from the original search
            titulo (str): Titulo parameter from the original search
            max_retries (int): Maximum number of retries if session expires

        Returns:
            dict: Dictionary containing the patent details or None if failed
        """
        # Check if session is valid
        if not self.check_and_renew_session():
            print('Session is not authenticated')
            return None

        params = {
            'Action': 'detail',
            'CodPedido': patent_id,
            'SearchParameter': search_param,
            'Resumo': resumo,
            'Titulo': titulo
        }

        try:
            try:
                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=10
                )
            except requests.exceptions.Timeout:
                print(f"Request timed out for patent {patent_id}, returning partial info")
                return {
                    'patent_id': patent_id,
                }

            if response.status_code != 200:
                print(f"Failed to retrieve patent details: {response.status_code}")
                return None

            # Check if we got a login page
            if self.is_login_page(response.text):
                print(f"Session expired while retrieving details for patent {patent_id}.")
                return None

            # Successfully got the detail page
            detail_content = response.text

            # Save the detail page content
            self._save_detail_content(detail_content, patent_id)

            # Debug mode - open in browser if enabled
            if self.debug:
                self._debug_response(response, f"detail_{patent_id}")

            # Parse the details page
            parse_detail = self._parse_detail_page(detail_content)
            if parse_detail == {}:
                print('Parse detail page returned empty')
            return self._parse_detail_page(detail_content)

        except Exception as e:
            print(f"Error retrieving patent details for {patent_id}: {e}")
            print(f"Could not retrieve patent details for {patent_id}.")
            return None

    def _parse_detail_page(self, html_content):
        """
        Parse the details page HTML content

        Args:
            html_content (str): HTML content of the detail page

        Returns:
            dict: Dictionary containing the patent details
        """
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract details from the detail page
        details = {}

        # Extract patent number (código de pedido BR XX XXXX XXXXXX X)
        patent_number_elem = soup.select_one("font.marcador")
        if patent_number_elem:
            details['patent_number_full'] = self._remove_line_breaks(patent_number_elem.text.strip())

        # Extract filing date (data do depósito)
        filing_date_row = soup.find("font", string=lambda text: text and "Data do Depósito:" in text)
        if filing_date_row:
            filing_date_elem = filing_date_row.find_next("font", class_="normal")
            if filing_date_elem:
                details['filing_date_detail'] = self._remove_line_breaks(filing_date_elem.text.strip())

        # Extract publication date if available
        pub_date_row = soup.find("font", string=lambda text: text and "Data da Publicação:" in text)
        if pub_date_row:
            pub_date_elem = pub_date_row.find_next("font", class_="normal")
            if pub_date_elem:
                pub_date = pub_date_elem.text.strip().replace('-', '').strip()
                details['publication_date'] = self._remove_line_breaks(pub_date) if pub_date else None

        # Extract grant date if available
        grant_date_row = soup.find("font", string=lambda text: text and "Data da Concessão:" in text)
        if grant_date_row:
            grant_date_elem = grant_date_row.find_next("font", class_="normal")
            if grant_date_elem:
                grant_date = grant_date_elem.text.strip().replace('-', '').strip()
                details['grant_date'] = self._remove_line_breaks(grant_date) if grant_date else None

        # Extract IPC classifications if available
        ipc_rows = soup.find_all("a", href="javascript:void(0)", onmouseout=lambda x: x and "hideMe('classificacao" in x)
        ipc_codes = []
        for i, row in enumerate(ipc_rows):
            if 'normal' in row.get('class', []) and row.text.strip():
                ipc_codes.append(self._remove_line_breaks(row.text.strip()))

        if ipc_codes:
            details['ipc_codes'] = ipc_codes

        # Extract title
        title_context = soup.select_one("div#tituloContext")
        if title_context:
            title_text = title_context.get_text(strip=True)
            if title_text:
                details['title'] = self._remove_line_breaks(title_text)

        # Extract abstract
        abstract_context = soup.select_one("div#resumoContext")
        if abstract_context:
            abstract_text = abstract_context.get_text(strip=True)
            if abstract_text:
                details['abstract'] = self._remove_line_breaks(abstract_text)

        # Extract applicants (depositantes)
        applicant_row = soup.find("font", string=lambda text: text and "Nome do Depositante:" in text)
        if applicant_row:
            applicant_elem = applicant_row.find_next("font", class_="normal")
            if applicant_elem:
                applicants_text = applicant_elem.text.strip()
                details['applicants'] = [self._remove_line_breaks(app.strip()) for app in applicants_text.split('/')]
                details['applicants_raw'] = self._remove_line_breaks(applicants_text)

        # Extract inventors if available
        inventor_row = soup.find("font", string=lambda text: text and "Nome do Inventor:" in text)
        if inventor_row:
            inventor_elem = inventor_row.find_next("font", class_="normal")
            if inventor_elem:
                inventors_text = inventor_elem.text.strip()
                details['inventors'] = [self._remove_line_breaks(inv.strip()) for inv in inventors_text.split('/')]
                details['inventors_raw'] = self._remove_line_breaks(inventors_text)

        # Extract patent agent if available
        agent_row = soup.find("font", string=lambda text: text and "Nome do Procurador:" in text)
        if agent_row:
            agent_elem = agent_row.find_next("font", class_="normal")
            if agent_elem:
                details['patent_agent'] = self._remove_line_breaks(agent_elem.text.strip())

        # Extract publications/despachos (office actions)
        publications = []
        pub_table = soup.select_one("div.accordion-item input#accordion-3 + label + div.accordion-content table")
        if pub_table:
            pub_rows = pub_table.select("tr.normal")
            for row in pub_rows:
                rpi_elem = row.select_one("td:nth-of-type(1) font.normal")
                date_elem = row.select_one("td:nth-of-type(2) font.normal b")
                code_elem = row.select_one("td:nth-of-type(3) font.normal a")
                # Look for PDF icon
                pdf_elem = row.select_one("td:nth-of-type(4) img[src*='iconePdf.png']")
                complement_elem = row.select_one("td:nth-of-type(6) font.normal")

                if rpi_elem and date_elem and code_elem:
                    pub = {
                        'rpi': self._remove_line_breaks(rpi_elem.text.strip()),
                        'date': self._remove_line_breaks(date_elem.text.strip()),
                        'code': self._remove_line_breaks(code_elem.text.strip()),
                        'has_pdf': bool(pdf_elem),
                        'complement': self._remove_line_breaks(complement_elem.text.strip() if complement_elem else '')
                    }
                    publications.append(pub)

        # Convert publications to JSON string for storage in single field
        if publications:
            details['publications_json'] = json.dumps(publications, ensure_ascii=False)

        # Extract petitions (petições)
        petitions = []
        pet_table = soup.select_one("div.accordion-item input#accordion-1 + label + div.accordion-content table")
        if pet_table:
            petition_sections = pet_table.find_all("font", class_="titulo", string=lambda x: x and x.strip() in ["Serviços", "Anuidade", "Outros"])
            for section in petition_sections:
                section_tr = section.find_parent("tr")

                # Get all petition rows after this section heading and before the next section
                petition_rows = []
                current = section_tr.find_next_sibling("tr")
                while current and not current.find("font", class_="titulo"):
                    if 'normal' in current.get('class', []):
                        petition_rows.append(current)
                    current = current.find_next_sibling("tr")

                for row in petition_rows:
                    service_elem = row.select_one("td:nth-of-type(1) font.normal a")
                    payment_elem = row.select_one("td:nth-of-type(2) img[alt*='Pagamento']")
                    protocol_elem = row.select_one("td:nth-of-type(3) font.normal")
                    date_elem = row.select_one("td:nth-of-type(4) font.normal")
                    client_elem = row.select_one("td:nth-of-type(8) font.normal")

                    if service_elem and protocol_elem and date_elem:
                        service_code = service_elem.text.strip()
                        petition = {
                            'section': self._remove_line_breaks(section.text.strip()),
                            'service_code': self._remove_line_breaks(service_code),
                            'has_payment': bool(payment_elem),
                            'protocol': self._remove_line_breaks(protocol_elem.text.strip()),
                            'date': self._remove_line_breaks(date_elem.text.strip()),
                            'client': self._remove_line_breaks(client_elem.text.strip() if client_elem else '')
                        }
                        petitions.append(petition)

        # Convert petitions to JSON string for storage in single field
        if petitions:
            details['petitions_json'] = json.dumps(petitions, ensure_ascii=False)

        # Extract anuidades (fees)
        anuidades = {}
        anuidade_table = soup.select_one("div.accordion-item input#accordion-2 + label + div.accordion-content table")
        if anuidade_table:
            # Get the status of anuidades (fees) using the images
            anuidade_imgs = anuidade_table.select("a[href*='javascript:void(0)'] img[alt*='Anuidade']")
            for img in anuidade_imgs:
                if img.find_previous("font", class_="normal"):
                    anuidade_num = img.find_previous("font", class_="normal").text.strip()
                    anuidade_status = "Paga" if "Averbada" in img.get("alt", "") else "Não Paga"
                    anuidade_num = anuidade_num.split("ª")[0] if "ª" in anuidade_num else anuidade_num
                    anuidades[f"anuidade_{anuidade_num}"] = anuidade_status

        if anuidades:
            details['anuidades_json'] = json.dumps(anuidades, ensure_ascii=False)

        # Extract last update date
        update_date_elem = soup.find("font", string=lambda text: text and "Dados atualizados até" in text)
        if update_date_elem:
            date_match = re.search(r'atualizados até\s+<b>\s*(\d{2}/\d{2}/\d{4})\s*</b>', str(update_date_elem))
            if date_match:
                details['last_update_date'] = self._remove_line_breaks(date_match.group(1))

        return details

    def _save_detail_content(self, html_content, patent_id):
        """
        Save the detail page HTML content to a cache folder

        Args:
            html_content (str): The HTML content to save
            patent_id (str): The patent ID
        """
        # Create cache directory if it doesn't exist
        cache_dir = "inpi_cache/details"
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        # Generate a filename based on the patent ID
        filename = f"{cache_dir}/patent_{patent_id}.html"

        # Save the HTML content
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)

    def fetch_all_details(self, max_patents=None, delay=True, continue_on_error=False):
        """
        Fetch details for all patents in the results

        Args:
            max_patents (int, optional): Maximum number of patents to fetch details for. If None, fetch all.
            delay (bool): Whether to add a delay between requests
            continue_on_error (bool): Whether to continue if a detail fetch fails

        Returns:
            list: List of dictionaries containing patent details
        """
        if not self.patents:
            print("No patents to fetch details for. Run search() first.")
            return []

        patents_to_process = self.patents
        if max_patents:
            patents_to_process = self.patents[:max_patents]

        total = len(patents_to_process)
        print(f"Fetching details for {total} patents...")

        self.detailed_patents = []

        # Track failures
        failures = []

        for i, patent in enumerate(patents_to_process):
            print(f"Fetching details for patent {i+1}/{total}: {patent['patent_number']}")

            patent_id = patent.get('patent_id')

            # Check if already fully processed (in CSV with details)
            if patent_id in self.csv_patents_dict and self.csv_patents_dict[patent_id].get('has_details', False):
                print(f"  Skipping already processed patent {patent['patent_number']} - already has details in CSV")
                continue

            # Add a delay between requests to be polite to the server
            if delay and i > 0:
                time.sleep(1.0)  # random.uniform(1.0, 3.0))

            # Check if session is still valid
            if not self.check_and_renew_session():
                if continue_on_error:
                    print(f"  Failed to maintain session for patent {patent['patent_number']}. Adding to failures list.")
                    failures.append(patent)
                    continue
                else:
                    print("Session expired and could not be renewed. Saving progress and exiting.")
                    # Save any details collected so far
                    if self.detailed_patents:
                        self.append_to_csv()
                    return self.detailed_patents

            # Fetch details
            details = self.get_patent_details(
                patent['patent_id'],
                search_param=patent.get('search_param', ''),
                resumo='',
                titulo=''
            )

            if details:
                # Combine basic info with details, but keep original info if it conflicts
                combined = patent.copy()
                for key, value in details.items():
                    if key not in combined:
                        combined[key] = value
                self.detailed_patents.append(combined)

                # Update processed patents set
                self.processed_patent_ids.add(patent_id)

                # Save intermittently to avoid losing data on interruptions
                if i % 10 == 0 and i > 0 and self.detailed_patents:
                    print(f"Saving intermediate results ({len(self.detailed_patents)} patents)")
                    self.append_to_csv()
            else:
                print(f"FAILED to fetch details for patent {patent['patent_number']}")
                if continue_on_error:
                    failures.append(patent)
                else:
                    print("Stopping due to failure. Saving progress.")
                    # Save any details collected so far
                    if self.detailed_patents:
                        self.append_to_csv()
                    return self.detailed_patents

        if failures:
            print(f"\nFailed to fetch details for {len(failures)} patents:")
            for patent in failures:
                print(f"  {patent['patent_number']} (ID: {patent['patent_id']})")

        print(f"Successfully fetched details for {len(self.detailed_patents)} patents")
        return self.detailed_patents

    def append_to_csv(self):
        """
        Append the newly scraped patents to an existing CSV file.
        If the file doesn't exist, create it.

        Args:
            filename (str): Name of the CSV file

        Returns:
            DataFrame: The DataFrame containing the newly appended data
        """
        filename = self.csv_file
        if not self.detailed_patents:
            print("No detailed patents to save.")
            return None

        # Convert to DataFrame
        df_new = pd.DataFrame(self.detailed_patents)
        required_columns = ['patent_number', 'filing_date', 'patent_id', 'title', 'ipc', 'patent_number_raw', 'search_param', 'patent_number_full', 'filing_date_detail',
                            'publication_date', 'grant_date', 'applicants', 'applicants_raw', 'patent_agent', 'ipc_codes', 'abstract', 'inventors_raw', 'inventors']
        for column in required_columns:
            if column not in df_new.columns:
                df_new[column] = None

        # Columns to drop (as requested)
        columns_to_drop = [
            'detail_url',
            'first_publication_rpi', 'first_publication_date', 'first_publication_code', 'first_publication_complement',
            'first_petition_service_code', 'first_petition_protocol', 'first_petition_date', 'first_petition_client',
            'publications', 'petitions'  # These are complex objects
        ]

        # Drop columns if they exist
        df_new = df_new.drop(columns=[col for col in columns_to_drop if col in df_new.columns], errors='ignore')

        # Check if file exists
        file_exists = os.path.isfile(filename)

        if file_exists:
            # Load existing data to check if we need to append
            df_existing = pd.read_csv(filename)

            # Only append if we have new data
            if not df_new.empty:
                # Check if any of the new patents are already in the existing data
                if 'patent_id' in df_existing.columns:
                    existing_ids = set(df_existing['patent_id'].astype(str))
                    df_new = df_new[~df_new['patent_id'].astype(str).isin(existing_ids)]

                # Append only if we have new data after filtering
                if not df_new.empty:
                    # Make sure columns match
                    all_columns = set(df_existing.columns) | set(df_new.columns)

                    # Add missing columns to both dataframes
                    for col in all_columns:
                        if col not in df_existing.columns:
                            df_existing[col] = None
                        if col not in df_new.columns:
                            df_new[col] = None

                    # Reorder columns to match
                    df_new = df_new[df_existing.columns]

                    # Append to CSV
                    df_new.to_csv(filename, mode='a', header=False, index=False, encoding='utf-8')
                    print(f"Appended {len(df_new)} new patents to {filename}")

                    # Update our tracking dictionary
                    for _, row in df_new.iterrows():
                        patent_id = str(row['patent_id'])
                        self.csv_patents_dict[patent_id] = {
                            'patent_number': row.get('patent_number', ''),
                            'has_details': 'patent_agent' in df_new.columns and not pd.isna(row.get('patent_agent', '')),
                            'row': row.to_dict()
                        }

                        # Add to processed ids
                        self.processed_patent_ids.add(patent_id)

                    # Clear detailed_patents after saving to avoid duplicate appends
                    self.detailed_patents = []

                    # Return combined data for reference
                    return pd.concat([df_existing, df_new], ignore_index=True)
                else:
                    print("No new patents to append.")
                    return df_existing
            else:
                print("No new patents to append.")
                return df_existing
        else:
            # Create new file
            df_new.to_csv(filename, index=False, encoding='utf-8')
            print(f"Created new file {filename} with {len(df_new)} patents")

            # Update our tracking dictionary
            for _, row in df_new.iterrows():
                patent_id = str(row['patent_id'])
                self.csv_patents_dict[patent_id] = {
                    'patent_number': row.get('patent_number', ''),
                    'has_details': 'patent_agent' in df_new.columns and not pd.isna(row.get('patent_agent', '')),
                    'row': row.to_dict()
                }

                # Add to processed ids
                self.processed_patent_ids.add(patent_id)

            # Clear detailed_patents after saving to avoid duplicate appends
            self.detailed_patents = []

            return df_new

    def is_authenticated(self):
        """Check if the current session is authenticated"""
        try:
            test_url = "https://busca.inpi.gov.br/pePI/jsp/patentes/PatenteSearchBasico.jsp"
            response = self.session.get(test_url)

            # Check for indicators of being logged in
            auth_indicator = "Finalizar Sessão" in response.text

            # Check if we're seeing a login page
            login_page = self.is_login_page(response.text)

            if login_page:
                self.session_expired = True
                return False

            self.session_expired = not auth_indicator
            return auth_indicator
        except Exception as e:
            print(f"Error checking authentication: {e}")
            self.session_expired = True
            return False

    def _debug_response(self, response, label="debug"):
        """Debug helper to save and open responses"""
        if not self.debug:
            return

        # Generate a unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"inpi_{label}_{timestamp}.html"

        # Save the response HTML to a file
        with open(filename, 'wb') as f:
            f.write(response.content)

        file_path = os.path.abspath(filename)
        print(f"Debug: Saved response to {file_path}")

        # Open the file in the default browser
        print(f"Debug: Opening {label} response in browser...")
        webbrowser.open('file://' + file_path)


# Example usage
if __name__ == "__main__":
    # Create the parser
    parser = argparse.ArgumentParser(description="INPI Scraper")

    # Add positional arguments
    parser.add_argument("search_column", help="Search column")
    parser.add_argument("text_to_search", help="Text to search")

    # Parse the arguments
    args = parser.parse_args()

    # Access the arguments
    print(f"Search column: {args.search_column}")
    print(f"Text to search: {args.text_to_search}")

    suffix = f'{args.search_column.replace(" ", "")}-{args.text_to_search.replace(" ", "")}'

    # File paths for data storage
    output_file = f"inpi_combined_patents_{suffix}.csv"
    state_file = f"inpi_search_state_{suffix}.json"

    # Create scraper with cookies and debug mode (set to False for production)
    scraper = INPIPatentScraper(cookies=COOKIES_STRING, debug=False, csv_file=output_file, state_file=state_file)

    if not scraper.is_authenticated():
        print("Failed to authenticate. Exiting.")
        sys.exit(1)

    # Load existing data and search state to avoid re-scraping
    scraper.load_existing_data(csv_filename=output_file, state_filename=state_file)

    # Will continue from last page processed if available
    results = scraper.search(args.text_to_search, search_column=args.search_column, max_pages=200, continue_from_last=True)

    # Show the first few results from the search
    if results is not None and not results.empty:
        print("\nSearch Results Preview:")
        print(results[['patent_number', 'filing_date']].head())

        # Fetch details for all new patents found and append to CSV periodically
        detailed_patents = scraper.fetch_all_details(continue_on_error=False)

        # Final append to CSV for any remaining patents
        if scraper.detailed_patents:
            combined_df = scraper.append_to_csv()

            # Print summary
            if combined_df is not None:
                print(f"\nTotal patents in database: {len(combined_df)}")
        else:
            print("No new details to append.")

        # Update search state one last time
        scraper.save_search_state()
    else:
        if not scraper.search_state['has_more_pages']:
            print("All pages have been processed. Search is complete.")
        else:
            print("No new patents found on the pages processed, or search failed.")
