# services/contact_db_search.py
"""Contact search using contacts.db SQLite database for Azerbaijani contact queries"""
import os
import re
import sqlite3

def enhance_rag_with_contact_search(rag_service_instance):
    """Wrap the RAG service to handle contact queries via contacts.db"""
    original = rag_service_instance.answer_question
    # Find the contacts.db file - check multiple possible locations
    possible_paths = [
        '/tmp/contacts.db',  # Vercel Lambda (restored from Blob Storage on startup)
        os.path.join(os.path.dirname(__file__), '..', 'contacts.db'),  # backend/contacts.db (local)
        os.path.join(os.path.dirname(os.getcwd()), 'contacts.db'),  # Parent directory
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'contacts.db'),  # Project root
        os.path.join(os.getcwd(), 'contacts.db'),  # Current directory (last resort)
    ]
    # Normalise paths so os.path.exists works reliably
    possible_paths = [os.path.normpath(p) for p in possible_paths]
    
    db_path = None
    for path in possible_paths:
        if os.path.exists(path):
            db_path = path
            break

    def _extract_name(question: str) -> str:
        # Exclude general search keywords and job titles from name extraction
        general_keywords = [
            'Hamı', 'Bütün', 'Kim', 'Siyahı', 'Telefon', 'Nömrə', 'Məlumat', 
            'Nazir', 'Müdir', 'Rəis', 'Sədr', 'Direktor',
            'Şöbə', 'İdarə', 'Sektor', 'Bölmə', 'Şöbə', 'Xidmət',
            'Mütəxəssis', 'Məsləhətçi', 'Ekspert', 'Mühasib', 'Mühəndis',
            'Auditor', 'Müfəttiş', 'Köməkçi', 'Müşavir', 'Hüquqşünas',
            'Koordinator', 'Menecer', 'Katib', 'Qeydiyyatçı', 'Referent',
            'Operator', 'Müəllim', 'Anbardar', 'Rəhbər'
        ]
        
        # Full name pattern: First Last
        match = re.search(r"\b[A-ZƏÇĞÖÜŞİ][a-zəçöüşğı]+\s+[A-ZƏÇĞÖÜŞİ][a-zəçöüşğı]+\b", question)
        if match and match.group(0) not in general_keywords:
            return match.group(0)
        # Fallback: single capitalized name
        match = re.search(r"\b[A-ZƏÇĞÖÜŞİ][a-zəçöüşğı]{3,}\b", question)
        if match and match.group(0) not in general_keywords:
            return match.group(0)
        return ""

    def _detect_info_type(question: str) -> list:
        q = question.lower()
        types = []
        if 'mobil' in q or 'telefon' in q:
            types.append('Mobil')
        if 'daxili' in q:
            types.append('Daxili')
        if 'şəhər' in q:
            types.append('Şəhər')
        if 'vəzifə' in q or 'işi' in q or 'məsul' in q:
            types.append('Vəzifə')
        if 'şöbə' in q or 'departament' in q:
            types.append('Şöbə')
        if 'sektor' in q or 'bölüm' in q:
            types.append('Sektor')
        if 'mail' in q or 'email' in q:
            types.append('Email')
        if not types:
            types = ['Ad', 'Soyad', 'Vəzifə', 'Şöbə', 'Sektor', 'Mobil', 'Daxili', 'Şəhər', 'Email']
        return types

    def _is_list_query(question: str) -> bool:
        """Check if user wants a list of people"""
        q = question.lower()
        list_keywords = ['siyahı', 'list', 'hamı', 'bütün', 'neçə', 'kim var', 'kimdir', 'kimləri', 'işləyən', 'çalışan']
        return any(keyword in q for keyword in list_keywords)
    
    def _extract_department_or_sector(question: str) -> tuple:
        """Extract department or sector name from question (departmentində işləyən işçilər pattern)"""
        q = question.lower()
        department = None
        sector = None
        
        # Match patterns like "xxx departmentində işləyən" or "xxx departmentinda işləyən"
        dept_match = re.search(r'(.+?)\s+departmentində', q)
        if not dept_match:
            dept_match = re.search(r'(.+?)\s+departmentinda', q)
        if dept_match:
            department = dept_match.group(1).strip()
        
        # Match patterns like "xxx sektorda işləyən" or "xxx sektarında işləyən" (including typos)
        # This handles multi-word sectors like "Kadr siyasəti sektarında"
        sector_match = re.search(r'(.+?)\s+sektor[a-zıouəəəəə]*(?:nda|nə|da|də)?(?:\s+|$)', q)
        if not sector_match:
            # Specific match for "sektarında" variant (typo)
            sector_match = re.search(r'(.+?)\s+sektarında', q)
        if not sector_match:
            # Match "sektorda" variant
            sector_match = re.search(r'(.+?)\s+sektorda', q)
        if not sector_match:
            # Fallback: "sektor" keyword followed by name
            sector_match = re.search(r'sektor[a-z]*\s+(?:işləyən\s+)?(.+?)(?:\s+|$)', q)
        if not sector_match:
            # Another pattern: sector name at end with "sektoru"
            sector_match = re.search(r'(.+?)\s+sektoru(?:\s+|$)', q)
        
        if sector_match:
            sector = sector_match.group(1).strip()
        
        return department, sector

    def _search_multiple_contacts(conn, name_part: str, info_types: list) -> list:
        """Search for multiple contacts with partial name matching"""
        cur = conn.cursor()
        
        # Search by partial name in both Ad and Soyad
        cur.execute(
            """SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts
               WHERE lower(Ad) LIKE ? OR lower(Soyad) LIKE ?
               ORDER BY Ad, Soyad""",
            (f'%{name_part.lower()}%', f'%{name_part.lower()}%')
        )
        
        rows = cur.fetchall()
        results = []
        
        for row in rows:
            parts = []
            for key in info_types:
                if key in row.keys() and row[key] and row[key] != 'yoxdur':
                    parts.append(f"{key}: {row[key]}")
            
            if not parts:
                # Show all available info if specific type not found
                for key in ['Ad', 'Soyad', 'Vəzifə', 'Şöbə', 'Sektor', 'Mobil', 'Daxili', 'Şəhər', 'Email']:
                    if row[key] and row[key] != 'yoxdur':
                        parts.append(f"{key}: {row[key]}")
            
            contact_info = f"**{row['Ad']} {row['Soyad']}**\n" + "\n".join(parts)
            results.append(contact_info)
        
        return results
    
    def _search_by_department_or_sector(conn, department: str = None, sector: str = None, info_types: list = None) -> list:
        """Search for contacts by department/office or sector"""
        if not info_types:
            info_types = ['Ad', 'Soyad', 'Vəzifə', 'Şöbə', 'Sektor', 'Mobil', 'Daxili', 'Şəhər', 'Email']
        
        cur = conn.cursor()
        results = []
        
        if department:
            # Search by department (Şöbə column)
            cur.execute(
                """SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts
                   WHERE lower(Şöbə) LIKE ? OR lower(Şöbə) LIKE ?
                   ORDER BY Ad, Soyad""",
                (f'%{department.lower()}%', f'%{department.lower()}%')
            )
            rows = cur.fetchall()
            
            for row in rows:
                parts = []
                for key in info_types:
                    if key in row.keys() and row[key] and row[key] != 'yoxdur':
                        parts.append(f"{key}: {row[key]}")
                
                if not parts:
                    for key in ['Ad', 'Soyad', 'Vəzifə', 'Şöbə', 'Sektor', 'Mobil', 'Daxili', 'Şəhər', 'Email']:
                        if row[key] and row[key] != 'yoxdur':
                            parts.append(f"{key}: {row[key]}")
                
                contact_info = f"**{row['Ad']} {row['Soyad']}**\n" + "\n".join(parts)
                results.append(contact_info)
        
        elif sector:
            # Search by sector
            cur.execute(
                """SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts
                   WHERE lower(Sektor) LIKE ?
                   ORDER BY Ad, Soyad""",
                (f'%{sector.lower()}%',)
            )
            rows = cur.fetchall()
            
            for row in rows:
                parts = []
                for key in info_types:
                    if key in row.keys() and row[key] and row[key] != 'yoxdur':
                        parts.append(f"{key}: {row[key]}")
                
                if not parts:
                    for key in ['Ad', 'Soyad', 'Vəzifə', 'Şöbə', 'Sektor', 'Mobil', 'Daxili', 'Şəhər', 'Email']:
                        if row[key] and row[key] != 'yoxdur':
                            parts.append(f"{key}: {row[key]}")
                
                contact_info = f"**{row['Ad']} {row['Soyad']}**\n" + "\n".join(parts)
                results.append(contact_info)
        
        return results

    def enhanced_answer_question(question: str, doc_id: int):
        lower_q = question.lower()
        # detect contact query - expanded keywords
        contact_keywords = [
            'telefon', 'nömrə', 'mobil', 'daxili', 'şəhər', 'əlaqə', 'kimin',
            'işçi', 'əməkdaş', 'siyahı', 'list', 'hamı', 'bütün', 'vəzifə', 'müdir',
            'mütəxəssis', 'məsləhətçi', 'rəis', 'baş', 'çıxart', 'göstər', 'tap',
            'şöbə', 'departament', 'sektor', 'bölüm', 'işləyən', 'çalışan', 'email', 'mail'
        ]
        
        if any(k in lower_q for k in contact_keywords):
            print(f"🔍 Contact query detected: {question}")
            
            # Check for department/sector search
            department, sector = _extract_department_or_sector(question)
            
            if department or sector:
                print(f"🏢 Department/Sector query: department={department}, sector={sector}")
                if not db_path or not os.path.exists(db_path):
                    return {'answer': f'contacts.db tapılmadı. Checked paths: {possible_paths}'}
                
                try:
                    conn = sqlite3.connect(db_path)
                    conn.row_factory = sqlite3.Row
                    info_types = _detect_info_type(question)
                    
                    results = _search_by_department_or_sector(conn, department, sector, info_types)
                    conn.close()
                    
                    if not results:
                        dept_or_sect = department or sector
                        return {'answer': f'"{dept_or_sect}" şöbə/sektorda heç bir əməkdaş tapılmadı.'}
                    
                    # Format multiple results
                    if len(results) == 1:
                        answer = results[0]
                    else:
                        answer = f"**{len(results)} əməkdaş tapıldı:**\n\n" + "\n\n".join(results)
                    
                    print(f"Department/Sector contacts found: {len(results)}")
                    return {'answer': answer}
                    
                except Exception as e:
                    print(f"Database error: {e}")
                    return {'answer': f'Verilənlər bazası xətası: {str(e)}'}
            
            # Check if this is a list query (multiple results)
            is_list_query = _is_list_query(question)
            
            name = _extract_name(question)
            
            # Check if this is a job title search without specific name
            job_keywords = [
                'müdir', 'rəis', 'nazir', 'müavin', 'mütəxəssis', 'məsləhətçi', 'baş',
                'direktor', 'sədr', 'koordinator', 'inspektor', 'ekspert',
                'anbardar', 'rəhbər', 'mühasib', 'mühəndis', 'auditor', 'müfəttiş',
                'köməkçi', 'müşavir', 'hüquqşünas', 'kob dostu', 'kargüzar',
                'menecer', 'katib', 'qeydiyyatçı', 'referent', 'texniki', 'operator', 'müəllim'
            ]
            job_search = any(keyword in lower_q for keyword in job_keywords)
            general_search = any(word in lower_q for word in ['hamı', 'bütün', 'kim var', 'siyahı', 'telefon nömrələri'])
            
            if not name:
                # Try to extract from context words
                words = question.split()
                for i, word in enumerate(words):
                    if word.lower() in ['kim', 'kimin', 'adında', 'soyadı'] and i + 1 < len(words):
                        potential_name = words[i + 1]
                        if len(potential_name) > 2 and potential_name[0].isupper():
                            # Don't treat job titles as names
                            if potential_name.lower() not in job_keywords:
                                name = potential_name
                                break
                
                # If still no specific name and it's not a job/general search, ask for clarification
                if not name and not job_search and not general_search:
                    return {'answer': 'Şəxsin adı tapılmadı. Zəhmət olmasa Ad və Soyad daxil edin.'}
            
            print(f"Extracted name: '{name}' (List query: {is_list_query}, Job search: {job_search})")
            info_types = _detect_info_type(question)
            print(f"Info types requested: {info_types}")
            
            # query database
            if not db_path or not os.path.exists(db_path):
                return {'answer': f'contacts.db tapılmadı. Checked paths: {possible_paths}'}
            
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                
                # Handle job title searches
                if job_search and not name:
                    # Check for specific compound job titles first
                    if 'nazir müavin' in lower_q or 'nazir müavini' in lower_q:
                        # Specifically search for deputy ministers
                        cur.execute(
                            "SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts "
                            "WHERE lower(Vəzifə) LIKE '%nazir müavini%' OR lower(Vəzifə) LIKE '%nazir müavin%' "
                            "OR lower(Vəzifə) LIKE '%nazirin müavini%' OR lower(Vəzifə) LIKE '%nazirin müavin%' "
                            "ORDER BY Ad, Soyad"
                        )
                    elif 'müdir müavin' in lower_q or 'müdir müavini' in lower_q:
                        # Specifically search for deputy directors
                        cur.execute(
                            "SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts "
                            "WHERE lower(Vəzifə) LIKE '%müdir müavini%' OR lower(Vəzifə) LIKE '%müdir müavin%' "
                            "ORDER BY Ad, Soyad"
                        )
                    else:
                        # General job keyword search
                        job_terms = []
                        for keyword in job_keywords:
                            if keyword in lower_q:
                                job_terms.append(keyword)
                        
                        conditions = []
                        params = []
                        for term in job_terms:
                            conditions.append("lower(Vəzifə) LIKE ?")
                            params.append(f'%{term}%')
                        
                        query_sql = f"SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts WHERE {' OR '.join(conditions)} ORDER BY Ad, Soyad"
                        cur.execute(query_sql, params)
                    rows = cur.fetchall()
                    
                    results = []
                    for row in rows:
                        parts = []
                        for key in info_types:
                            if key in row.keys() and row[key] and row[key] != 'yoxdur':
                                parts.append(f"{key}: {row[key]}")
                        
                        if not parts:
                            for key in ['Ad', 'Soyad', 'Vəzifə', 'Şöbə', 'Sektor', 'Mobil', 'Daxili', 'Şəhər', 'Email']:
                                if row[key] and row[key] != 'yoxdur':
                                    parts.append(f"{key}: {row[key]}")
                        
                        contact_info = f"**{row['Ad']} {row['Soyad']}**\n" + "\n".join(parts)
                        results.append(contact_info)
                
                # Handle general "all contacts" searches
                elif general_search and not name:
                    cur.execute(
                        "SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts ORDER BY Ad, Soyad"
                    )
                    rows = cur.fetchall()
                    results = []
                    
                    for row in rows:
                        parts = []
                        for key in info_types:
                            if key in row.keys() and row[key] and row[key] != 'yoxdur':
                                parts.append(f"{key}: {row[key]}")
                        
                        if not parts:
                            for key in ['Ad', 'Soyad', 'Vəzifə', 'Şöbə', 'Sektor', 'Mobil', 'Daxili', 'Şəhər', 'Email']:
                                if row[key] and row[key] != 'yoxdur':
                                    parts.append(f"{key}: {row[key]}")
                        
                        contact_info = f"**{row['Ad']} {row['Soyad']}**\n" + "\n".join(parts)
                        results.append(contact_info)
                
                # Handle name-based searches
                elif name:
                    if is_list_query:
                        # Search for all contacts with partial name match
                        results = _search_multiple_contacts(conn, name, info_types)
                    else:
                        # Single contact search
                        parts = name.split()
                        ad = parts[0]
                        soyad = parts[1] if len(parts) > 1 else ''
                        
                        # Try exact match first - try both name orders
                        row = None
                        if soyad:
                            # Try Ad=first, Soyad=second (e.g., "Anar Axundov")
                            cur.execute(
                                "SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts"
                                " WHERE lower(Ad)=? AND lower(Soyad)=?",
                                (ad.lower(), soyad.lower())
                            )
                            row = cur.fetchone()
                            
                            # If not found, try Ad=second, Soyad=first (e.g., "Axundov Anar")
                            if not row:
                                cur.execute(
                                    "SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts"
                                    " WHERE lower(Ad)=? AND lower(Soyad)=?",
                                    (soyad.lower(), ad.lower())
                                )
                                row = cur.fetchone()
                        else:
                            # Search by single name in both Ad and Soyad columns
                            cur.execute(
                                "SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts"
                                " WHERE lower(Ad)=? OR lower(Soyad)=?",
                                (ad.lower(), ad.lower())
                            )
                            row = cur.fetchone()
                        
                        # If still not found, try partial matching
                        if not row:
                            if soyad:
                                cur.execute(
                                    "SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts"
                                    " WHERE lower(Ad) LIKE ? OR lower(Soyad) LIKE ? OR lower(Ad) LIKE ? OR lower(Soyad) LIKE ?",
                                    (f'%{ad.lower()}%', f'%{soyad.lower()}%', f'%{soyad.lower()}%', f'%{ad.lower()}%')
                                )
                            else:
                                cur.execute(
                                    "SELECT Ad, Soyad, Vəzifə, Şöbə, Sektor, Mobil, Daxili, Şəhər, Email FROM contacts"
                                    " WHERE lower(Ad) LIKE ? OR lower(Soyad) LIKE ?",
                                    (f'%{ad.lower()}%', f'%{ad.lower()}%')
                                )
                            row = cur.fetchone()
                        
                        if not row:
                            conn.close()
                            return {'answer': f'"{name}" adında əməkdaş tapılmadı.'}
                        
                        # build response for single contact
                        parts = []
                        for key in info_types:
                            if key in row.keys() and row[key] and row[key] != 'yoxdur':
                                parts.append(f"{key}: {row[key]}")
                        
                        if not parts:
                            # Show all available info if specific type not found
                            parts = []
                            for key in ['Ad', 'Soyad', 'Vəzifə', 'Şöbə', 'Sektor', 'Mobil', 'Daxili', 'Şəhər', 'Email']:
                                if row[key] and row[key] != 'yoxdur':
                                    parts.append(f"{key}: {row[key]}")
                        
                        answer = f"**{row['Ad']} {row['Soyad']}**\n" + "\n".join(parts)
                        conn.close()
                        print(f"Contact found: {answer}")
                        return {'answer': answer}
                
                else:
                    conn.close()
                    return {'answer': 'Axtarış parametrləri aydın deyil.'}
                
                conn.close()
                
                if not results:
                    return {'answer': 'Heç bir əməkdaş tapılmadı.'}
                
                # Format multiple results
                if len(results) == 1:
                    answer = results[0]
                else:
                    answer = f"**{len(results)} əməkdaş tapıldı:**\n\n" + "\n\n".join(results)
                
                print(f"Multiple contacts found: {len(results)}")
                return {'answer': answer}
                
            except Exception as e:
                print(f"Database error: {e}")
                return {'answer': f'Verilənlər bazası xətası: {str(e)}'}
        
        # fallback to original RAG
        return original(question, doc_id)

    rag_service_instance.answer_question = enhanced_answer_question
    return rag_service_instance
