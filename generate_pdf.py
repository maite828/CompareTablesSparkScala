#!/usr/bin/env python3
"""
Simple script to convert README_TABLE_COMPARISON.md to HTML with PDF-friendly styling.
Open the generated HTML in a browser and use Print to PDF.
"""

import re
import sys
from pathlib import Path

def markdown_to_html(md_content):
    """Basic markdown to HTML converter with table support."""
    
    # Convert headers
    md_content = re.sub(r'^#### (.*?)$', r'<h4>\1</h4>', md_content, flags=re.MULTILINE)
    md_content = re.sub(r'^### (.*?)$', r'<h3>\1</h3>', md_content, flags=re.MULTILINE)
    md_content = re.sub(r'^## (.*?)$', r'<h2>\1</h2>', md_content, flags=re.MULTILINE)
    md_content = re.sub(r'^# (.*?)$', r'<h1>\1</h1>', md_content, flags=re.MULTILINE)
    
    # Convert bold and italic
    md_content = re.sub(r'\*\*\*(.*?)\*\*\*', r'<strong><em>\1</em></strong>', md_content)
    md_content = re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', md_content)
    md_content = re.sub(r'\*(.*?)\*', r'<em>\1</em>', md_content)
    
    # Convert inline code
    md_content = re.sub(r'`([^`]+)`', r'<code>\1</code>', md_content)
    
    # Convert code blocks
    md_content = re.sub(
        r'```(\w+)?\n(.*?)```', 
        r'<pre><code>\2</code></pre>', 
        md_content, 
        flags=re.DOTALL
    )
    
    # Convert lists
    lines = md_content.split('\n')
    in_ul = False
    in_ol = False
    result = []
    
    for line in lines:
        # Unordered lists
        if re.match(r'^\s*[-*]\s+', line):
            if not in_ul:
                result.append('<ul>')
                in_ul = True
            item = re.sub(r'^\s*[-*]\s+', '', line)
            result.append(f'<li>{item}</li>')
        # Ordered lists
        elif re.match(r'^\s*\d+\.\s+', line):
            if not in_ol:
                result.append('<ol>')
                in_ol = True
            item = re.sub(r'^\s*\d+\.\s+', '', line)
            result.append(f'<li>{item}</li>')
        else:
            if in_ul:
                result.append('</ul>')
                in_ul = False
            if in_ol:
                result.append('</ol>')
                in_ol = False
            result.append(line)
    
    if in_ul:
        result.append('</ul>')
    if in_ol:
        result.append('</ol>')
    
    md_content = '\n'.join(result)
    
    # Convert links
    md_content = re.sub(r'\[(.*?)\]\((.*?)\)', r'<a href="\2">\1</a>', md_content)
    
    # Convert tables
    table_lines = []
    in_table = False
    result_lines = []
    
    for line in md_content.split('\n'):
        if '|' in line and line.strip().startswith('|'):
            if not in_table:
                table_lines = ['<table border="1" cellpadding="5" cellspacing="0">']
                in_table = True
            
            # Skip separator line
            if re.match(r'^\|[\s\-:|]+\|$', line):
                continue
            
            cells = [c.strip() for c in line.strip().split('|')[1:-1]]
            
            # First row is header
            if len(table_lines) == 1:
                table_lines.append('<thead><tr>')
                for cell in cells:
                    table_lines.append(f'<th>{cell}</th>')
                table_lines.append('</tr></thead><tbody>')
            else:
                table_lines.append('<tr>')
                for cell in cells:
                    table_lines.append(f'<td>{cell}</td>')
                table_lines.append('</tr>')
        else:
            if in_table:
                table_lines.append('</tbody></table>')
                result_lines.extend(table_lines)
                in_table = False
                table_lines = []
            result_lines.append(line)
    
    if in_table:
        table_lines.append('</tbody></table>')
        result_lines.extend(table_lines)
    
    md_content = '\n'.join(result_lines)
    
    # Convert paragraphs
    md_content = re.sub(r'\n\n+', '</p><p>', md_content)
    md_content = f'<p>{md_content}</p>'
    
    return md_content

def create_html(title, content):
    """Create full HTML document with PDF-friendly styling."""
    
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        @page {{
            size: A4;
            margin: 2cm;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1000px;
            margin: 0 auto;
            padding: 20px;
            background: white;
        }}
        
        h1 {{
            color: #EC0000;
            border-bottom: 3px solid #EC0000;
            padding-bottom: 10px;
            page-break-after: avoid;
        }}
        
        h2 {{
            color: #4D4D4D;
            border-bottom: 2px solid #EC0000;
            padding-bottom: 8px;
            margin-top: 30px;
            page-break-after: avoid;
        }}
        
        h3 {{
            color: #666666;
            margin-top: 25px;
            page-break-after: avoid;
        }}
        
        h4 {{
            color: #666666;
            margin-top: 20px;
            page-break-after: avoid;
        }}
        
        code {{
            background-color: #f5f5f5;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            color: #EC0000;
        }}
        
        pre {{
            background-color: #4D4D4D;
            color: #ffffff;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
            page-break-inside: avoid;
        }}
        
        pre code {{
            background-color: transparent;
            color: #ffffff;
            padding: 0;
        }}
        
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
            page-break-inside: avoid;
        }}
        
        th {{
            background-color: #EC0000;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: bold;
        }}
        
        td {{
            padding: 10px;
            border: 1px solid #ddd;
        }}
        
        tr:nth-child(even) {{
            background-color: #f5f5f5;
        }}
        
        ul, ol {{
            margin: 15px 0;
            padding-left: 30px;
        }}
        
        li {{
            margin: 8px 0;
        }}
        
        strong {{
            color: #4D4D4D;
        }}
        
        em {{
            color: #666666;
        }}
        
        a {{
            color: #EC0000;
            text-decoration: none;
        }}
        
        a:hover {{
            text-decoration: underline;
        }}
        
        .page-break {{
            page-break-before: always;
        }}
        
        @media print {{
            body {{
                max-width: 100%;
            }}
            
            a {{
                color: #000;
                text-decoration: none;
            }}
            
            pre {{
                border: 1px solid #ccc;
            }}
        }}
    </style>
</head>
<body>
{content}
</body>
</html>"""
    
    return html

def main():
    # Read README
    readme_path = Path(__file__).parent / "README.md"
    
    if not readme_path.exists():
        print(f"Error: {readme_path} not found!")
        sys.exit(1)
    
    print(f"Reading {readme_path}...")
    with open(readme_path, 'r', encoding='utf-8') as f:
        md_content = f.read()
    
    # Convert to HTML
    print("Converting Markdown to HTML...")
    html_content = markdown_to_html(md_content)
    
    # Create full HTML document
    full_html = create_html("Table Comparator - Documentation", html_content)
    
    # Write output
    output_path = Path(__file__).parent / "README.html"
    print(f"Writing {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(full_html)
    
    print(f"\n✅ Success! HTML generated: {output_path}")
    print("\nTo create PDF:")
    print("1. Open README.html in your browser")
    print("2. Press Ctrl+P (Print)")
    print("3. Select 'Save as PDF' as destination")
    print("4. Click Save")

if __name__ == "__main__":
    main()
