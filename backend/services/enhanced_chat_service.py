# services/enhanced_chat_service.py (UPDATED)
"""Enhanced chat service with improved document detection and matching"""
import json
import re
import unicodedata
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

# Import the improved document matching system
from services.improved_document_matching import ImprovedDocumentMatcher

class EnhancedChatService:
    """Smart chat service that can answer general questions and detect document needs"""
    
    def __init__(self, db_manager, rag_service, config):
        self.db_manager = db_manager
        self.rag_service = rag_service
        self.config = config
        
        # Initialize improved document matcher
        self.document_matcher = ImprovedDocumentMatcher(db_manager)
        
        # Download intent trigger words (Azerbaijani + English + common keyboard variants)
        self._download_triggers = {
            # template / sample
            'şablon', 'sablon', 'template', 'nümunə', 'numune', 'numuna', 'numuye',
            # download / send
            'yüklə', 'yukle', 'yükle', 'download', 'göndər', 'gonder',
            # get / obtain
            'ver', 'al', 'əldə', 'elde', 'tap', 'paylaş', 'paylas',
            # document / file
            'fayl', 'faylı', 'file', 'sənəd', 'sened',
            # need / want
            'lazım', 'lazim', 'istəyirəm', 'isteyirem', 'isteyirem',
            # link
            'link',
        }

    # ------------------------------------------------------------------
    # Azerbaijani-aware text normalisation helpers
    # ------------------------------------------------------------------
    _AZ_MAP = str.maketrans('əçğıöüş', 'ecgious')

    def _az_norm(self, text: str) -> str:
        """Lowercase + replace Azerbaijani special chars with basic Latin."""
        return text.lower().translate(self._AZ_MAP)

    def _tokenize(self, text: str) -> List[str]:
        """Split normalised text into tokens of length ≥ 2."""
        return [t for t in re.split(r'[^a-z0-9]+', self._az_norm(text)) if len(t) >= 2]

    # ------------------------------------------------------------------

    def find_template_by_keywords(self, question: str) -> Optional[Dict]:
        """
        Generic document-download matcher.
        Detects download intent from the question, then scores ALL uploaded
        documents by how well their names match the remaining question words.
        Works for any document the admin uploads – no hardcoded mappings.
        Returns: {'document': doc_dict, 'doc_display_name': str}  or  None
        """
        q_tokens = set(self._tokenize(question))
        trigger_tokens = {self._az_norm(t) for t in self._download_triggers}

        # 1. Bail out early if no download intent in the question
        if not (q_tokens & trigger_tokens):
            return None

        # 2. Load all documents
        documents = self.db_manager.get_documents()
        if not documents:
            return None

        # 3. Content tokens = question tokens minus trigger/stop words
        stop_tokens = trigger_tokens | {
            'bu', 'bir', 'de', 'da', 'ile', 've', 'mi', 'mu', 'bana',
            'mene', 'lutfen', 'xahis', 'zehmet', 'olsa', 'olar',
        }
        content_tokens = {t for t in q_tokens if t not in stop_tokens and len(t) >= 2}

        print(f"[Download] Content tokens: {content_tokens}")

        # 4. Score every document by token overlap against its filename
        best_doc = None
        best_score = 0

        for doc in documents:
            doc_name = doc.get('original_name', '')
            # Remove extension for matching, then tokenise
            base_name = re.sub(r'\.[^.]+$', '', doc_name)
            doc_tokens = set(self._tokenize(base_name))

            score = 0
            for qt in content_tokens:
                for dt in doc_tokens:
                    if qt == dt:
                        score += 10          # exact normalised match
                    elif qt in dt or dt in qt:
                        score += 6           # substring match
                    elif len(qt) >= 4 and len(dt) >= 4 and qt[:4] == dt[:4]:
                        score += 3           # 4-char prefix match

            print(f"  '{doc_name}' → score {score}")

            if score > best_score:
                best_score = score
                best_doc = doc

        # 5. If we have a winner above threshold, return it
        if best_doc and best_score >= 6:
            display_name = re.sub(r'\.[^.]+$', '', best_doc['original_name']).replace('_', ' ').replace('-', ' ')
            print(f"✓ Download match: '{best_doc['original_name']}' (score {best_score})")
            return {'document': best_doc, 'doc_display_name': display_name}

        # 6. If intent is clear but no name matched, fall back to listing all docs
        #    so the user can choose — but only if there's exactly one document
        if len(documents) == 1:
            doc = documents[0]
            display_name = re.sub(r'\.[^.]+$', '', doc['original_name']).replace('_', ' ').replace('-', ' ')
            return {'document': doc, 'doc_display_name': display_name}

        print("✗ Download intent detected but no document matched")
        return None

    def _are_similar_words(self, word1: str, word2: str) -> bool:
        """Check if two words are similar via Azerbaijani normalisation."""
        if len(word1) < 3 or len(word2) < 3:
            return False
        return self._az_norm(word1) == self._az_norm(word2)

    def find_relevant_document(self, question: str, documents: List[Dict]) -> Optional[int]:
        """Find the most relevant document using improved matching algorithm"""
        print(f"Searching for document matching question: '{question}'")
        
        # Use the enhanced document matching system
        doc_id = self.document_matcher.enhanced_document_matching(question, documents)
        
        if doc_id:
            matched_doc = next((d for d in documents if d['id'] == doc_id), None)
            if matched_doc:
                print(f"✓ Enhanced matching found: '{matched_doc['original_name']}'")
                return doc_id
        
        print("✗ Enhanced matching failed, trying fallback methods")
        
        # Fallback to original logic with improvements
        question_lower = question.lower()
        question_keywords = self._extract_enhanced_keywords(question)
        question_normalized = self._normalize_text(question)
        question_tokens = set(re.findall(r'[a-z0-9əçıöüşğ]+', question_normalized))
        
        # Check if document name is directly mentioned
        best_name_match = None
        best_name_score = 0
        for doc in documents:
            doc_name = doc['original_name']
            doc_name_lower = doc_name.lower()
            doc_name_without_ext = doc_name_lower.rsplit('.', 1)[0]
            doc_name_clean = re.sub(r'[_-]', ' ', doc_name_without_ext)
            doc_name_normalized = self._normalize_text(re.sub(r'\.[^.]+$', '', doc_name))
            doc_tokens = set(re.findall(r'[a-z0-9əçıöüşğ]+', doc_name_normalized))

            score = 0

            # Legacy checks
            if (doc_name_without_ext in question_lower or
                doc_name_lower in question_lower or
                any(part in question_lower for part in doc_name_clean.split() if len(part) > 3)):
                score += 30

            # Strong normalized phrase match
            if doc_name_normalized and doc_name_normalized in question_normalized:
                score += 100

            # Token overlaps (e.g., RİİS)
            for token in question_tokens.intersection(doc_tokens):
                if len(token) >= 3:
                    score += 25

            if score > best_name_score:
                best_name_score = score
                best_name_match = doc['id']

        if best_name_match and best_name_score >= 25:
            matched_doc = next((d for d in documents if d['id'] == best_name_match), None)
            if matched_doc:
                print(f"✓ Direct name match found: '{matched_doc['original_name']}' (score: {best_name_score})")
            return best_name_match
        
        # Enhanced keyword matching with scoring
        best_match = None
        best_score = 0
        
        for doc in documents:
            score = self._calculate_document_relevance_score(
                question, question_keywords, doc
            )
            
            if score > best_score and score >= 5:  # Minimum threshold
                best_score = score
                best_match = doc['id']
        
        if best_match:
            matched_doc = next((d for d in documents if d['id'] == best_match), None)
            print(f"✓ Keyword matching found: '{matched_doc['original_name']}' (score: {best_score})")
        else:
            print("✗ No suitable document found")
        
        return best_match

    def _normalize_text(self, text: str) -> str:
        """Normalize text for robust Unicode-insensitive filename matching."""
        text = (text or '').casefold()
        text = unicodedata.normalize('NFKD', text)
        text = ''.join(ch for ch in text if not unicodedata.combining(ch))
        text = re.sub(r'[_\-\./]+', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _extract_enhanced_keywords(self, question: str) -> List[str]:
        """Extract enhanced keywords from question"""
        # Remove common words with expanded list
        stop_words = {
            'və', 'ya', 'ilə', 'üçün', 'olan', 'olur', 'edir', 'etməkl', 'bu', 'o', 'bir',
            'nə', 'hansı', 'kim', 'harada', 'niyə', 'necə', 'the', 'is', 'at', 'which', 
            'on', 'and', 'a', 'an', 'as', 'are', 'də', 'da', 'ki', 'ya', 'yaxud', 
            'amma', 'lakin', 'çünki', 'həm', 'hər', 'bəzi', 'çox', 'az'
        }
        
        # Extract words with better pattern
        words = re.findall(r'\b[a-zA-ZəçöüşğıƏÇÖÜŞĞI]+\b', question.lower())
        keywords = [w for w in words if w not in stop_words and len(w) > 2]
        
        # Add named entities (potential person names)
        name_pattern = r'\b[A-ZƏÇĞÖÜŞÄİ][a-zəçöüşğı]+(?:\s+[A-ZƏÇĞÖÜŞÄİ][a-zəçöüşğı]+)*\b'
        names = re.findall(name_pattern, question)
        for name in names:
            keywords.extend(name.lower().split())
        
        # Extract phone numbers if present
        phone_pattern = r'\b(050|055|051|070|077)[-\s]?\d{3}[-\s]?\d{2}[-\s]?\d{2}\b|\b\d{3}[-.]?\d{3}[-.]?\d{2,4}\b'
        phone_matches = re.findall(phone_pattern, question)
        keywords.extend([phone for phone_tuple in phone_matches for phone in phone_tuple if phone])
        
        return keywords

    def _calculate_document_relevance_score(self, question: str, question_keywords: List[str], doc: Dict) -> float:
        """Calculate enhanced relevance score for document"""
        score = 0
        question_lower = question.lower()
        doc_name = doc['original_name'].lower()
        doc_type = doc.get('document_type', '')
        
        # Enhanced keyword matching from database
        if doc.get('keywords'):
            try:
                doc_keywords = json.loads(doc['keywords'])
                
                # Exact matches (higher weight)
                exact_matches = sum(1 for q_kw in question_keywords 
                                  if any(q_kw == d_kw.lower() for d_kw in doc_keywords))
                score += exact_matches * 3
                
                # Partial matches (lower weight)
                for q_kw in question_keywords:
                    for d_kw in doc_keywords:
                        d_kw_lower = d_kw.lower()
                        if len(q_kw) > 3 and len(d_kw_lower) > 3:
                            if q_kw in d_kw_lower or d_kw_lower in q_kw:
                                score += 1
                
            except json.JSONDecodeError:
                pass
        
        # Document type enhanced matching
        type_keywords = {
            'contact': {
                'primary': ['telefon', 'əlaqə', 'nömrə', 'mobil', 'kim', 'hansı', 'çağırmaq', 'şöbə'],
                'context_patterns': [
                    r'\b(kim|kimin|hansı\s+\w+).*\b(telefon|nömrə|mobil|daxili)\b',
                    r'\b(telefon|nömrə|mobil|daxili)\b.*\b(kim|kimin|hansı)\b',
                    r'\b[A-ZƏÇĞÖÜŞÄİ][a-zəçöüşğı]+\s+[A-ZƏÇĞÖÜŞÄİ][a-zəçöüşğı]+\b.*\b(telefon|nömrə)\b'
                ]
            },
            'vacation': ['məzuniyyət', 'istirahət', 'tətil', 'gün'],
            'contract': ['müqavilə', 'razılaşma', 'saziş', 'şərt'],
            'business_trip': ['ezamiyyət', 'səfər', 'komandirovka'],
            'memorandum': ['memorandum', 'anlaşma', 'razılaşma']
        }
        
        if doc_type in type_keywords:
            type_config = type_keywords[doc_type]
            
            if isinstance(type_config, dict):
                # Contact document with enhanced matching
                primary_keywords = type_config.get('primary', [])
                patterns = type_config.get('context_patterns', [])
                
                # Primary keyword matches
                primary_matches = sum(1 for kw in primary_keywords if kw in question_lower)
                score += primary_matches * 5
                
                # Pattern matches (very high weight for contact documents)
                for pattern in patterns:
                    if re.search(pattern, question_lower):
                        score += 8
                        
            elif isinstance(type_config, list):
                # Other document types
                type_matches = sum(1 for kw in type_config if kw in question_lower)
                score += type_matches * 4
        
        # File type relevance
        file_type = doc.get('file_type', '').lower()
        type_keywords_file = {
            'pdf': ['pdf', 'sənəd', 'fayl', 'document'],
            'docx': ['word', 'docx', 'məktub', 'letter'],
            'xlsx': ['excel', 'cədvəl', 'statistika', 'rəqəm', 'table', 'data'],
            'txt': ['mətn', 'text', 'txt', 'note'],
            'json': ['json', 'data', 'məlumat', 'api']
        }
        
        if file_type in type_keywords_file:
            for keyword in type_keywords_file[file_type]:
                if keyword in question_lower:
                    score += 2
        
        # Special handling for contact documents with person names
        if doc_type == 'contact' or 'telefon' in doc_name:
            # Boost score if question contains person names
            name_pattern = r'\b[A-ZƏÇĞÖÜŞÄİ][a-zəçöüşğı]+\s+[A-ZƏÇĞÖÜŞÄİ][a-zəçöüşğı]+\b'
            if re.search(name_pattern, question):
                score += 4
            
            # Boost for phone-related questions
            phone_indicators = ['telefon', 'nömrə', 'mobil', 'daxili', 'çağır', 'zəng', 'əlaqə']
            if any(indicator in question_lower for indicator in phone_indicators):
                score += 5
        
        # Penalize if document has too many random numbers (poor keyword extraction)
        if doc.get('keywords'):
            try:
                doc_keywords = json.loads(doc['keywords'])
                numeric_keywords = [kw for kw in doc_keywords if str(kw).isdigit()]
                if len(numeric_keywords) > len(doc_keywords) * 0.6:  # More than 60% numbers
                    score -= 3
            except:
                pass
        
        return score

    def is_document_related_question(self, question: str) -> bool:
        """Enhanced document detection with better patterns"""
        doc_indicators = [
            'sənəd', 'fayl', 'document', 'file', 'pdf', 'excel', 'word',
            'cədvəl', 'məktub', 'hesabat', 'report', 'table', 'data',
            'yüklənmiş', 'uploaded', 'saxlanmış', 'stored',
            '.pdf', '.docx', '.xlsx', '.txt', '.json',
            'məlumat', 'tapın', 'göstərin', 'axtarın', 'haqqında',
            'içində', 'daxilində', 'faylda', 'sənəddə',
            'telefon', 'nömrə', 'əlaqə', 'kim', 'hansı'  # Contact-specific indicators
        ]
        
        question_lower = question.lower()
        
        # Check for direct indicators
        for indicator in doc_indicators:
            if indicator in question_lower:
                return True
        
        # Enhanced patterns for document queries
        doc_patterns = [
            r'\b\w+\.(pdf|docx?|xlsx?|txt|json)\b',  # File names with extensions
            r'\b(bu|həmin|o)\s+(sənəd|fayl)',  # References like "bu sənəd"
            r'(nə|kim|necə|harada|niyə).*\b(yazılıb|qeyd|göstərilib)',  # Document content queries
            r'\b[A-ZƏÇĞÖÜŞÄİ][a-zəçöüşğı]+\s+[A-ZƏÇĞÖÜŞÄİ][a-zəçöüşğı]+\b.*\b(telefon|nömrə|əlaqə)\b',  # Person + contact
            r'\b(kim|kimin|hansı).*\b(telefon|nömrə|mobil|daxili)\b',  # Who + phone questions
        ]
        
        for pattern in doc_patterns:
            if re.search(pattern, question_lower):
                return True
        
        # Check if question mentions specific departments or positions (likely in contact docs)
        dept_position_indicators = [
            'müdir', 'rəis', 'şöbə', 'sektor', 'idarə', 'bölmə', 'mütəxəssis',
            'koordinator', 'məsul', 'köməkçi', 'operator', 'katib'
        ]
        
        if any(indicator in question_lower for indicator in dept_position_indicators):
            return True
        
        return False
    
    def answer_general_question(self, question: str) -> str:
        """Answer general questions using OpenAI without document context"""
        try:
            prompt = f"""
Sen Azərbaycan dilində cavab verən AI assistentsən.
Sualı diqqətlə oxu və uyğun cavab ver.

Sual: {question}

Qeydlər:
- Cavabı yalnız Azərbaycan dilində yaz
- Dəqiq və faydalı məlumat ver
- Əgər sual konkret sənəd və ya fayl haqqındadırsa, bildirin ki sənəd yüklənməyib
- Nəzakətli və peşəkar ol

Cavab:"""

            response = self.rag_service.model.generate_content(prompt)
            return response.text
            
        except Exception as e:
            return f"Üzr istəyirəm, cavab verərkən xəta baş verdi: {str(e)}"
    
    def process_chat_message(self, question: str, user_id: int, conversation_id: Optional[int] = None) -> Dict:
        """Enhanced chat message processing with improved document detection"""
        print(f"\n=== Processing chat message ===")
        print(f"Question: '{question}'")
        print(f"User ID: {user_id}")
        
        # Check for contact queries FIRST - bypass document matching
        contact_keywords = ['telefon', 'nömrə', 'mobil', 'daxili', 'şəhər', 'əlaqə', 'kim', 'kimin']
        if any(keyword in question.lower() for keyword in contact_keywords):
            print("🔍 Contact query detected - using contact database search")
            # Use RAG service directly (which includes contact search)
            result = self.rag_service.answer_question(question, None)  # No document ID needed for contacts
            answer = result.get('answer', 'Əlaqə məlumatı tapılmadı')
            
            # Save conversation
            conv_id = self._save_conversation(user_id, question, answer, None, 'Contact Database', conversation_id)
            
            return {
                'answer': answer,
                'conversation_id': conv_id,
                'type': 'contact_answer'
            }
        
        # Check for template download requests 
        template_match = self.find_template_by_keywords(question)
        if template_match:
            print("✓ Template request detected")
            return self._handle_template_request(template_match, question, user_id, conversation_id)
        
        # Get user info
        user = self.db_manager.get_user_by_id(user_id)
        
        # Get ALL documents (both admin and user uploaded)
        all_documents = self.db_manager.get_documents()
        print(f"Available documents: {len(all_documents)}")
        
        # Enhanced document-related question detection
        is_doc_question = self.is_document_related_question(question)
        print(f"Is document question: {is_doc_question}")
        
        # More aggressive document search - try to find relevant document
        doc_id = None
        if all_documents:
            doc_id = self.find_relevant_document(question, all_documents)
            print(f"Found relevant document: {doc_id}")
        
        # If we found a document or it's clearly a document question
        if doc_id or (is_doc_question and all_documents):
            
            if not doc_id and is_doc_question:
                # Can't determine which document - ask for clarification
                print("✗ Document question but no specific match - asking for clarification")
                return {
                    'needs_clarification': True,
                    'available_documents': [
                        {'id': d['id'], 'name': d['original_name']} 
                        for d in all_documents
                    ],
                    'message': f'Sistemdə {len(all_documents)} sənəd var. Hansı sənəddən məlumat axtarırsınız?',
                    'type': 'clarification_needed'
                }
            
            if doc_id:
                # Found document - use RAG to answer
                doc = next((d for d in all_documents if d['id'] == doc_id), None)
                print(f"Using document: '{doc['original_name']}'")
                
                if not doc.get('is_processed'):
                    return {
                        'answer': f"'{doc['original_name']}' sənədi hələ işlənməyib. Zəhmət olmasa bir az gözləyin.",
                        'type': 'document_not_processed'
                    }
                
                # Get answer from RAG
                result = self.rag_service.answer_question(question, doc_id)
                answer = result.get('answer', 'Cavab tapılmadı')
                
                # Add source info
                answer_with_source = f"**Mənbə:** {doc['original_name']}\n\n{answer}"
                
                # Save conversation and get ID
                conv_id = self._save_conversation(user_id, question, answer_with_source, doc_id, doc['original_name'], conversation_id)
                
                return {
                    'answer': answer_with_source,
                    'conversation_id': conv_id,
                    'document_used': {
                        'id': doc['id'],
                        'name': doc['original_name']
                    },
                    'type': 'document_answer'
                }
        
        # No documents exist and question seems document-related
        if not all_documents and is_doc_question:
            answer = "Sistemdə heç bir sənəd yüklənməyib. Sənədlər yükləndikdən sonra onlar haqqında sual verə bilərsiniz. Bu arada başqa suallarınız varsa, məmnuniyyətlə cavablandıra bilərəm."
            conv_id = self._save_conversation(user_id, question, answer, None, None, conversation_id)
            
            return {
                'answer': answer,
                'type': 'no_documents',
                'conversation_id': conv_id
            }
        
        # General question - answer without document context
        print("✓ Processing as general question")
        answer = self.answer_general_question(question)
        
        # Save conversation and get ID
        conv_id = self._save_conversation(user_id, question, answer, None, None, conversation_id)
        
        return {
            'answer': answer,
            'conversation_id': conv_id,
            'type': 'general_answer'
        }
    
    def _handle_template_request(self, template_match: Dict, question: str, user_id: int, conversation_id: Optional[int]) -> Dict:
        """Handle template download requests"""
        document = template_match['document']
        template_info = template_match['template_info']
        
        # Create download URL
        download_url = f"http://localhost:5000/api/documents/{document['id']}/download"
        
        # Create response with proper markdown link format
        answer = f"""**{template_info['template_name']} nümunəsi** tapıldı!

🔥 **Yükləmə linki:** [Bu linkə klikləyin]({download_url})

📄 **Fayl məlumatları:**
- Fayl adı: {document['original_name']}
- Fayl tipi: {document['file_type']}
- Yüklənmə tarixi: {document['created_at']}

Linkə klikləyərək faylı kompüterinizə yükləyə bilərsiniz."""

        # Save conversation
        conv_id = self._save_conversation(user_id, question, answer, document['id'], document['original_name'], conversation_id)
        
        return {
            'answer': answer,
            'conversation_id': conv_id,
            'document_used': {
                'id': document['id'],
                'name': document['original_name']
            },
            'type': 'template_download'
        }
    
    def _save_conversation(self, user_id: int, question: str, answer: str, 
                          doc_id: Optional[int], doc_name: Optional[str], 
                          conversation_id: Optional[int]) -> int:
        """Save conversation to database"""
        message = {
            'question': question,
            'answer': answer,
            'document_id': doc_id,
            'document_name': doc_name,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        if conversation_id:
            # Update existing conversation
            conv = self.db_manager.get_conversation(conversation_id, user_id)
            if conv:
                messages = json.loads(conv['messages'])
                messages.append(message)
                self.db_manager.update_conversation(conversation_id, json.dumps(messages))
                return conversation_id
        
        # Create new conversation
        title = f"{doc_name}: {question[:30]}..." if doc_name else question[:50] + "..."
        new_conversation_id = self.db_manager.create_conversation(
            user_id=user_id,
            document_id=doc_id,
            title=title,
            messages=json.dumps([message])
        )
        
        return new_conversation_id
