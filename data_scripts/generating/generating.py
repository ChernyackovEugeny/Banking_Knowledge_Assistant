import yaml
import json
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

client = OpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    base_url="https://api.deepseek.com",
)

with open("config.yaml") as f:
    config = yaml.safe_load(f)

SYSTEM_PROMPT = f"""
Ты — генератор внутрибанковской документации.
Ты создаёшь внутренние регламенты для вымышленного 
банка {config['bank']['name']}.

Справочник банка:
{yaml.dump(config['bank'], allow_unicode=True)}

Требования к стилю:
- Сухой канцелярский язык
- Многоуровневая нумерация (1.1, 1.1.1)
- Каждый пункт: действие + ответственный + срок
- Конкретные цифры, даты, пороги
- НЕ использовать "в установленном порядке" без конкретики
- Ссылки на нормативные акты: указывать конкретные пункты
"""

TEMPLATE = """
Структура документа:
[Гриф: Для служебного пользования]
[Номер: {doc_id}]
[Утверждён: Правление {bank_name}, {date}]

1. ОБЩИЕ ПОЛОЖЕНИЯ (цель, область, нормативные ссылки, 
   термины, ответственные)
2. ОСНОВНАЯ ЧАСТЬ (процедуры, этапы, сроки, пороги)
3. КОНТРОЛЬ И ОТЧЁТНОСТЬ
4. ЗАКЛЮЧИТЕЛЬНЫЕ ПОЛОЖЕНИЯ
"""

def generate_document(doc_spec):
    prompt = f"""
Сгенерируй внутренний регламент:
Номер: {doc_spec['id']}
Название: {doc_spec['title']}
Ответственное подразделение: {doc_spec['responsible_dept']}
Нормативные ссылки: {', '.join(doc_spec['references'])}

Документ должен раскрывать:
{chr(10).join(f'- {t}' for t in doc_spec['key_topics'])}

Включить таблицы:
{chr(10).join(f'- {t}' for t in doc_spec.get('tables', []))}

Целевой объём: ~{doc_spec['target_words']} слов.

{TEMPLATE.format(
    doc_id=doc_spec['id'],
    bank_name=config['bank']['name'],
    date='15.01.2025'
)}
"""
    response = client.chat.completions.create(
        model="deepseek-chat",
        max_tokens=8000,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )
    return response.choices[0].message.content

# Генерация всех документов
for doc_spec in config['documents']:
    print(f"Generating {doc_spec['id']}...")
    text = generate_document(doc_spec)
    
    output_path = f"generated_docs/{doc_spec['id']}.md"
    with open(output_path, 'w') as f:
        f.write(text)
    
    # Сохраняем метаданные
    meta = {
        "document_id": doc_spec['id'],
        "document_title": doc_spec['title'],
        "cluster": doc_spec['cluster'],
        "doc_type": doc_spec['doc_type'],
        "doc_subtype": doc_spec['doc_subtype'],
    }
    with open(f"generated_docs/{doc_spec['id']}_meta.json", 'w') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)