# services/enhanced_rag_service.py (UPDATED)
"""Enhanced RAG service with improved document matching"""
import os
import time
import json
import re
from typing import Optional, List, Dict

# Heavy/external imports - all conditional to survive broken packages on Vercel
try:
    from openai import OpenAI, RateLimitError
except (ImportError, ModuleNotFoundError) as _e:
    print(f"WARNING: openai import failed: {_e}")
    OpenAI = None
    RateLimitError = Exception

try:
    from langchain_text_splitters import RecursiveCharacterTextSplitter
except (ImportError, ModuleNotFoundError):
    RecursiveCharacterTextSplitter = None

try:
    from langchain_community.vectorstores import Chroma
except (ImportError, ModuleNotFoundError):
    Chroma = None

from services.file_processor import FileProcessor
from services.intelligent_keyword_extractor import IntelligentKeywordExtractor
from services.improved_document_matching import ImprovedDocumentMatcher

# Conditional import - HuggingFace embeddings only available when
# sentence-transformers and torch are installed
try:
    from langchain_community.embeddings import HuggingFaceEmbeddings
except (ImportError, ModuleNotFoundError):
    HuggingFaceEmbeddings = None


class _OpenAIResponseCompat:
    def __init__(self, text: str):
        self.text = text


class _OpenAIModelCompat:
    def __init__(self, client: OpenAI, model_name: str):
        self.client = client
        self.model_name = model_name

    def generate_content(self, prompt: str) -> _OpenAIResponseCompat:
        response = self.client.responses.create(
            model=self.model_name,
            input=prompt
        )

        text = getattr(response, 'output_text', None)
        if text:
            return _OpenAIResponseCompat(text)

        output_chunks = []
        for item in getattr(response, 'output', []) or []:
            for content_item in getattr(item, 'content', []) or []:
                chunk = getattr(content_item, 'text', None)
                if chunk:
                    output_chunks.append(chunk)

        return _OpenAIResponseCompat("\n".join(output_chunks).strip())


class _OpenAIEmbeddingsCompat:
    def __init__(self, client: OpenAI, model_name: str):
        self.client = client
        self.model_name = model_name

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []

        response = self.client.embeddings.create(
            model=self.model_name,
            input=texts
        )
        return [item.embedding for item in response.data]

    def embed_query(self, text: str) -> List[float]:
        vectors = self.embed_documents([text])
        return vectors[0] if vectors else []


class EnhancedRAGServiceV2:
    """Enhanced RAG system with improved document matching"""
    
    def __init__(self, config, db_manager):
        self.config = config
        self.db_manager = db_manager
        self.blob_storage = None  # type: ignore  # injected externally after construction
        
        # Initialize improved document matcher
        self.document_matcher = ImprovedDocumentMatcher(db_manager)
        
        # Configure OpenAI
        self.openai_client = OpenAI(api_key=config.OPENAI_API_KEY)
        self.model = _OpenAIModelCompat(self.openai_client, config.LLM_MODEL)
        
        # Initialize embeddings
        embedding_model = (config.EMBEDDING_MODEL or "text-embedding-3-small").strip()
        if embedding_model.startswith("text-embedding-"):
            self.embeddings = _OpenAIEmbeddingsCompat(self.openai_client, embedding_model)
        else:
            if HuggingFaceEmbeddings is None:
                raise ImportError(
                    "HuggingFace embeddings require 'sentence-transformers' and 'torch'. "
                    "Install them or set EMBEDDING_MODEL=text-embedding-3-small to use OpenAI."
                )
            self.embeddings = HuggingFaceEmbeddings(
                model_name=embedding_model or 'sentence-transformers/all-MiniLM-L6-v2'
            )
        
        # Initialize file processor and keyword extractor
        self.file_processor = FileProcessor()
        self.keyword_extractor = IntelligentKeywordExtractor()
        
        # Text splitter
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=config.CHUNK_SIZE,
            chunk_overlap=config.CHUNK_OVERLAP,
            separators=["\n\n", "\n", ".", "?", "!"]
        )

        self.vector_store = Chroma(
            persist_directory=config.VECTOR_DB_PATH,
            embedding_function=self.embeddings
        )
        
        # Ensure keywords column exists
        self._ensure_keywords_column()
    
    def _ensure_keywords_column(self):
        """Ensure keywords column exists in documents table"""
        try:
            self.db_manager.execute_query(
                "ALTER TABLE documents ADD COLUMN keywords TEXT"
            )
        except:
            pass  # Column already exists
    
    def process_document_from_bytes(self, file_content: bytes, doc_id: int, filename: str) -> bool:
        """Process document from bytes (for Vercel Blob Storage)"""
        import tempfile
        import os as os_module
        
        try:
            # Save bytes to temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=os_module.path.splitext(filename)[1]) as tmp_file:
                tmp_file.write(file_content)
                tmp_file_path = tmp_file.name
            
            # Process the temporary file
            result = self.process_document(tmp_file_path, doc_id)
            
            # Clean up temporary file
            try:
                os_module.remove(tmp_file_path)
            except:
                pass
            
            return result
            
        except Exception as e:
            print(f"Error processing document from bytes: {e}")
            return False
    
    def process_document(self, file_path: str, doc_id: int) -> bool:
        """Process document with intelligent keyword extraction"""
        try:
            print(f"Processing document ID {doc_id}: {file_path}")
            
            # Extract text
            text = self.file_processor.extract_text(file_path)
            if not text or not text.strip():
                print(f"No text extracted from {file_path}")
                return False
            
            print(f"Extracted {len(text)} characters of text")
            
            # Get document info
            doc_info = self.db_manager.execute_query(
                "SELECT original_name, document_type FROM documents WHERE id = ?",
                (doc_id,),
                fetch_one=True
            )
            
            if not doc_info:
                print(f"Document not found in database: {doc_id}")
                return False
            
            doc_dict = dict(doc_info)
            doc_name = doc_dict['original_name']
            doc_type = doc_dict.get('document_type', 'other')
            
            print(f"Document: {doc_name}, Type: {doc_type}")
            
            # Extract intelligent keywords
            keywords = self.keyword_extractor.extract_keywords(text, doc_name, doc_type)
            keywords_json = json.dumps(keywords, ensure_ascii=False)
            
            # Save keywords to database
            self.db_manager.execute_query(
                "UPDATE documents SET keywords = ? WHERE id = ?",
                (keywords_json, doc_id)
            )
            
            print(f"Extracted {len(keywords)} intelligent keywords: {keywords[:10]}...")
            
            # Create chunks with enhanced metadata
            chunks = self.text_splitter.split_text(text)
            if not chunks:
                print(f"No chunks created from {file_path}")
                return False
            
            print(f"Created {len(chunks)} text chunks")
            
            # Create enhanced metadata
            metadatas = self._create_enhanced_metadata(chunks, doc_name, doc_id, doc_type, keywords)
            
            # Create vector store
            vector_db_path = os.path.join(
                self.config.VECTOR_DB_PATH,
                f"doc_{doc_id}"
            )
            
            # Remove old vector store if exists
            if os.path.exists(vector_db_path):
                import shutil
                shutil.rmtree(vector_db_path)
                print(f"Removed old vector store: {vector_db_path}")
            
            # Create new vector store with enhanced chunks
            enhanced_chunks = self._enhance_chunks_with_context(chunks, keywords, doc_name)

            vector_store = Chroma.from_texts(
                texts=enhanced_chunks,
                embedding=self.embeddings,  # ВАЖНО: embedding, НЕ embedding_function
                persist_directory=vector_db_path
            )
            
            print(f"Created vector store with {len(enhanced_chunks)} chunks")
            
            # Mark as processed
            self.db_manager.execute_query(
                "UPDATE documents SET is_processed = TRUE WHERE id = ?",
                (doc_id,)
            )

            # Persist ChromaDB to Blob Storage so it survives across Lambda invocations
            if self.blob_storage and self.blob_storage.blob_enabled:
                self.blob_storage.sync_chroma_to_blob(doc_id, vector_db_path)
            
            print(f"Successfully processed document: {doc_name}")
            return True
            
        except Exception as e:
            print(f"Document processing error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _create_enhanced_metadata(self, chunks: List[str], doc_name: str, 
                                doc_id: int, doc_type: str, keywords: List[str]) -> List[Dict]:
        """Create enhanced metadata for chunks"""
        metadatas = []
        
        # Content type detection keywords
        contact_indicators = ['telefon', 'mobil', 'email', '@', 'daxili', 'şöbə']
        table_indicators = ['|', '===', 'cədvəl', 'table', 'sətir']
        header_indicators = ['başlıq', 'fəsil', 'bölmə', 'maddə']
        
        for i, chunk in enumerate(chunks):
            chunk_lower = chunk.lower()
            
            # Determine content type based on intelligent analysis
            content_type = self._determine_content_type(chunk_lower, doc_type)
            
            # Calculate relevance score for this chunk
            relevance_score = self._calculate_chunk_relevance(chunk_lower, keywords)
            
            # Extract chunk-specific keywords
            chunk_keywords = [kw for kw in keywords if kw.lower() in chunk_lower]
            
            metadatas.append({
                "chunk_id": i,
                "document_id": doc_id,
                "document_name": doc_name,
                "document_type": doc_type,
                "file_path": f"doc_{doc_id}",
                "content_type": content_type,
                "chunk_index": i,
                "total_chunks": len(chunks),
                "relevance_score": relevance_score,
                "chunk_keywords": json.dumps(chunk_keywords[:10], ensure_ascii=False),
                "has_contact_info": any(ind in chunk_lower for ind in contact_indicators),
                "has_table_data": any(ind in chunk_lower for ind in table_indicators),
                "has_headers": any(ind in chunk_lower for ind in header_indicators)
            })
        
        return metadatas
    
    def _determine_content_type(self, chunk_lower: str, doc_type: str) -> str:
        """Determine the type of content in chunk"""
        
        # Phone/contact pattern
        if re.search(r'\d{3}[-.]?\d{3}[-.]?\d{2,4}', chunk_lower) or '@' in chunk_lower:
            return "contact_information"
        
        # Table pattern
        if '|' in chunk_lower or 'cədvəl' in chunk_lower or chunk_lower.count('\t') > 3:
            return "tabular_data"
        
        # Header pattern
        if re.search(r'^[A-ZƏÇĞÖÜŞÄİ][A-Za-zəçöüşğıĞġıİ\s]+$', chunk_lower) or 'başlıq' in chunk_lower:
            return "header_section"
        
        # Document type specific content
        if doc_type == 'contact':
            return "contact_directory"
        elif doc_type == 'contract':
            return "contract_terms"
        elif doc_type == 'vacation':
            return "vacation_details"
        elif doc_type == 'business_trip':
            return "business_trip_info"
        
        return "general_content"
    
    def _calculate_chunk_relevance(self, chunk_lower: str, keywords: List[str]) -> float:
        """Calculate relevance score for chunk based on keywords"""
        if not keywords:
            return 0.5
        
        score = 0.0
        chunk_words = set(re.findall(r'\b\w+\b', chunk_lower))
        
        for keyword in keywords:
            kw_lower = keyword.lower()
            if kw_lower in chunk_lower:
                # Exact match gets higher score
                if kw_lower in chunk_words:
                    score += 1.0
                else:
                    score += 0.5
        
        # Normalize score
        max_possible_score = len(keywords)
        return min(score / max_possible_score, 1.0) if max_possible_score > 0 else 0.5
    
    def _enhance_chunks_with_context(self, chunks: List[str], keywords: List[str], doc_name: str) -> List[str]:
        """Enhance chunks with contextual information"""
        enhanced_chunks = []
        
        # Create context header
        context_header = f"Sənəd: {doc_name}\nAçar sözlər: {', '.join(keywords[:10])}\n\n"
        
        for i, chunk in enumerate(chunks):
            # Add context to chunk
            enhanced_chunk = f"{context_header}Hissə {i+1}:\n{chunk}"
            enhanced_chunks.append(enhanced_chunk)
        
        return enhanced_chunks
    
    def find_document_by_intelligent_keywords(self, question: str) -> Optional[int]:
        """UPDATED: Use improved document matching system"""
        print(f"Finding document using intelligent keywords for: '{question}'")
        
        # Use the enhanced document matching system
        doc_id = self.document_matcher.smart_document_search(question)
        
        if doc_id:
            print(f"✓ Smart search found document ID: {doc_id}")
        else:
            print("✗ Smart search did not find any matching document")
        
        return doc_id
    
    def search_relevant_content(self, question: str, doc_id: int, k: int = None) -> Optional[str]:
        """Search for relevant content with enhanced filtering"""
        try:
            print(f"Searching relevant content in document {doc_id} for: '{question}'")
            
            vector_db_path = os.path.join(
                self.config.VECTOR_DB_PATH,
                f"doc_{doc_id}"
            )
            
            if not os.path.exists(vector_db_path):
                print(f"Vector DB not found locally: {vector_db_path}")
                # Try to restore from Blob Storage (Vercel ephemeral filesystem)
                if self.blob_storage and self.blob_storage.blob_enabled:
                    print(f"Attempting to restore ChromaDB for doc_{doc_id} from Blob...")
                    restored = self.blob_storage.sync_chroma_from_blob(doc_id, vector_db_path)
                    if not restored or not os.path.exists(vector_db_path):
                        print(f"ChromaDB restore failed for doc_{doc_id}")
                        return None
                    print(f"ChromaDB for doc_{doc_id} restored successfully")
                else:
                    return None
            
            # Load vector store
            vector_store = Chroma(
                persist_directory=vector_db_path,
                embedding_function=self.embeddings
            )
            
            # Search with enhanced filtering
            k = k or self.config.SEARCH_RESULTS_COUNT
            
            # Get more results for filtering
            docs = vector_store.similarity_search(question, k=k*2)
            
            if not docs:
                print("No similar documents found in vector store")
                return None
            
            print(f"Found {len(docs)} similar chunks before filtering")
            
            # Filter and rank results by relevance
            filtered_docs = self._filter_and_rank_results(docs, question)
            
            # Take top k results
            top_docs = filtered_docs[:k]
            
            print(f"Using top {len(top_docs)} chunks after filtering")
            
            # Combine with intelligent ordering
            context = self._combine_results_intelligently(top_docs, question)
            
            return context
            
        except Exception as e:
            print(f"Search error: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _filter_and_rank_results(self, docs, question: str) -> List:
        """Filter and rank search results by relevance"""
        question_lower = question.lower()
        scored_docs = []
        
        for doc in docs:
            score = 0
            content = doc.page_content.lower()
            metadata = doc.metadata
            
            # Base relevance score from metadata
            if metadata.get('relevance_score'):
                score += float(metadata.get('relevance_score', 0)) * 2
            
            # Content type bonus
            content_type = metadata.get('content_type', '')
            if 'contact' in question_lower and 'contact' in content_type:
                score += 3
            elif 'table' in question_lower and 'tabular' in content_type:
                score += 3
            elif 'başlıq' in question_lower and 'header' in content_type:
                score += 2
            
            # Keyword presence bonus
            chunk_keywords = metadata.get('chunk_keywords', '[]')
            try:
                keywords = json.loads(chunk_keywords)
                for kw in keywords:
                    if kw.lower() in question_lower:
                        score += 1
            except:
                pass
            
            # Enhanced question type specific scoring
            if any(word in question_lower for word in ['kim', 'kimin', 'hansı']):
                # Name/person queries - prioritize contact info
                if metadata.get('has_contact_info'):
                    score += 3
                # Look for person names in content
                if re.search(r'\b[A-ZƏÇĞÖÜŞÄİ][a-zəçöüşğı]+\s+[A-ZƏÇĞÖÜŞÄİ][a-zəçöüşğı]+\b', content):
                    score += 2
            
            if any(word in question_lower for word in ['telefon', 'nömrə', 'mobil', 'daxili']):
                # Phone number queries
                if re.search(r'\d{3}[-.]?\d{3}[-.]?\d{2,4}', content):
                    score += 4
                if metadata.get('has_contact_info'):
                    score += 3
            
            if any(word in question_lower for word in ['nə', 'nədir', 'haqqında']):
                # Information queries - prioritize general content
                if content_type == 'general_content':
                    score += 1
            
            # Content quality indicators
            if len(content.strip()) > 100:  # Substantial content
                score += 1
            
            if content.count('\n') > 2:  # Structured content
                score += 0.5
            
            scored_docs.append((score, doc))
        
        # Sort by score (descending)
        scored_docs.sort(key=lambda x: x[0], reverse=True)
        
        print(f"Top scored chunks: {[(score, len(doc.page_content)) for score, doc in scored_docs[:3]]}")
        
        return [doc for score, doc in scored_docs]
    
    def _combine_results_intelligently(self, docs, question: str) -> str:
        """Combine search results in an intelligent order"""
        if not docs:
            return ""
        
        question_lower = question.lower()
        combined_parts = []
        
        # Group results by content type
        contact_docs = []
        table_docs = []
        header_docs = []
        general_docs = []
        
        for doc in docs:
            content_type = doc.metadata.get('content_type', 'general_content')
            if 'contact' in content_type:
                contact_docs.append(doc)
            elif 'tabular' in content_type:
                table_docs.append(doc)
            elif 'header' in content_type:
                header_docs.append(doc)
            else:
                general_docs.append(doc)
        
        # Combine based on query type
        if any(word in question_lower for word in ['telefon', 'nömrə', 'əlaqə', 'kim', 'hansı']):
            # Contact query - prioritize contact info
            for docs_group in [contact_docs, table_docs, general_docs, header_docs]:
                for doc in docs_group[:2]:  # Limit each group
                    combined_parts.append(doc.page_content)
        else:
            # General query - balanced approach
            for docs_group in [general_docs, contact_docs, table_docs, header_docs]:
                for doc in docs_group[:2]:
                    combined_parts.append(doc.page_content)
        
        result = "\n\n---\n\n".join(combined_parts[:5])  # Limit total results
        print(f"Combined {len(combined_parts)} chunks into context ({len(result)} characters)")
        return result
    
    def answer_question(self, question: str, doc_id: int) -> Dict:
        """Answer question about document with enhanced processing"""
        try:
            print(f"\n=== Answering question ===")
            print(f"Question: '{question}'")
            print(f"Document ID: {doc_id}")
            
            # Search for relevant content
            context = self.search_relevant_content(question, doc_id)
            
            if not context:
                return {
                    'success': False,
                    'answer': 'Sənəddən uyğun məlumat tapılmadı.',
                    'error': 'No relevant context found'
                }
            
            print(f"Found relevant context ({len(context)} characters)")
            
            # Get document info for better context
            doc_info = self.db_manager.execute_query(
                "SELECT original_name, document_type FROM documents WHERE id = ?",
                (doc_id,),
                fetch_one=True
            )
            
            doc_name = dict(doc_info)['original_name'] if doc_info else 'Unknown'
            doc_type = dict(doc_info).get('document_type', 'other') if doc_info else 'other'
            
            print(f"Document: {doc_name}, Type: {doc_type}")
            
            # Generate enhanced answer
            answer = self._generate_enhanced_answer(question, context, doc_name, doc_type)
            
            print(f"Generated answer ({len(answer)} characters)")
            
            return {
                'success': True,
                'answer': answer,
                'context_length': len(context),
                'document_name': doc_name,
                'document_type': doc_type
            }
            
        except Exception as e:
            print(f"Answer generation error: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'answer': f'Xəta baş verdi: {str(e)}',
                'error': str(e)
            }

    def _generate_enhanced_answer(self, question: str, context: str, doc_name: str, doc_type: str) -> str:
        """Generate enhanced answer with retry on 429 Too Many Requests"""

        type_instructions = {
            'contact': """
    - Əlaqə məlumatları üçün bütün uyğun nömrələri və şəxsləri göstər
    - Telefon, mobil, daxili nömrələri aydın şəkildə təqdim et
    - Şöbə və vəzifə məlumatlarını daxil et
    - Məlumatları strukturlaşdırılmış şəkildə (cədvəl formatında) göstər
    - Şəxs adlarını, vəzifələrini və əlaqə məlumatlarını birlikdə təqdim et
            """,
            'contract': """
    - Müqavilə şərtlərini aydın şəkildə izah et
    - Tarix, məbləğ və müddət kimi vacib məlumatları vurğula
    - Məsul şəxsləri və onların vəzifələrini qeyd et
            """,
            'vacation': """
    - Məzuniyyət müddəti, başlama və bitiş tarixlərini göstər
    - Məsul şəxsləri və təsdiq prosedurunu izah et
            """,
            'business_trip': """
    - Ezamiyyət məqsədi, müddəti və məkanını göstər
    - Məsul şəxslər və prosedurları izah et
            """
        }

        specific_instructions = type_instructions.get(doc_type, "")

        prompt = f"""
    Sənəd məzmunu:
    {context}

    Sual: {question}

    VACİB TƏLİMATLAR:
    1. Yalnız verilən sənəd məzmununa əsasən cavab ver
    2. Cavabı yalnız Azərbaycan dilində yaz
    3. Məlumatları strukturlaşdırılmış şəkildə təqdim et
    4. Sənəddə olmayan məlumat əlavə etmə

    Sənəd növü üçün xüsusi tələblər:
    {specific_instructions}

    CAVAB FORMATI:
    - Əgər çoxlu məlumat varsa, siyahı halında təqdim et
    - Vacib məlumatları **qalın** şriftlə yaz
    - Telefon nömrələrini, email-ləri və şəxs adlarını dəqiq göstər
    - Məlumatların mənbəyini qeyd et
    - Əgər sual şəxs haqqındadırsa, həmin şəxsin bütün məlumatlarını (ad, vəzifə, şöbə, telefon) birlikdə göstər

    Cavab:"""

        # --- ВАЖНО: обработка ошибок и повтор ---
        for attempt in range(3):  # максимум 3 попытки
            try:
                response = self.model.generate_content(prompt)
                answer = response.text
                answer = self._post_process_answer(answer, question, doc_type)
                return answer
            except RateLimitError:
                print(f"⚠️ OpenAI rate limit hit (attempt {attempt + 1}/3). Waiting 25s...")
                time.sleep(25)
            except Exception as e:
                if "429" in str(e):
                    print(f"⚠️ OpenAI 429 Too Many Requests, retrying after 25s... ({attempt + 1}/3)")
                    time.sleep(25)
                    continue
                else:
                    print(f"Answer generation error: {e}")
                    return f"Cavab yaradarkən xəta: {str(e)}"

        return "OpenAI API çox yüklənib, zəhmət olmasa bir qədər sonra yenidən cəhd edin."
    
    def _post_process_answer(self, answer: str, question: str, doc_type: str) -> str:
        """Post-process answer for better formatting"""
        
        # For contact documents, ensure phone numbers are highlighted
        if doc_type == 'contact':
            # Highlight phone numbers
            phone_pattern = r'\b(\d{3}[-.]?\d{3}[-.]?\d{2,4})\b'
            answer = re.sub(phone_pattern, r'**\1**', answer)
            
            # Highlight mobile numbers
            mobile_pattern = r'\b(050|055|051|070|077)[-\s]?(\d{3})[-\s]?(\d{2})[-\s]?(\d{2})\b'
            answer = re.sub(mobile_pattern, r'**\1-\2-\3-\4**', answer)
        
        # Clean up extra whitespace
        answer = re.sub(r'\n\s*\n\s*\n', '\n\n', answer)
        
        return answer.strip()
    
    def delete_document_vectors(self, doc_id: int) -> bool:
        """Delete vector store for document"""
        try:
            vector_db_path = os.path.join(
                self.config.VECTOR_DB_PATH,
                f"doc_{doc_id}"
            )
            
            if os.path.exists(vector_db_path):
                import shutil
                shutil.rmtree(vector_db_path)
                print(f"Deleted vector store: {vector_db_path}")
                return True
            
            return False
            
        except Exception as e:
            print(f"Error deleting vectors: {e}")
            return False
