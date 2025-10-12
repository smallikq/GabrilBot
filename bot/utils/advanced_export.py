"""
Модуль расширенного экспорта данных в различные форматы
"""

import os
import json
import csv
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd


class AdvancedExporter:
    """Класс для экспорта данных в различные форматы"""
    
    @staticmethod
    def export_to_csv(df: pd.DataFrame, output_path: str, 
                     delimiter: str = ',', encoding: str = 'utf-8-sig') -> bool:
        """
        Экспорт данных в CSV формат
        
        Args:
            df: DataFrame с данными
            output_path: Путь для сохранения
            delimiter: Разделитель полей
            encoding: Кодировка файла
        
        Returns:
            True если успешно, False иначе
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            df.to_csv(
                output_path,
                index=False,
                sep=delimiter,
                encoding=encoding,
                quoting=csv.QUOTE_NONNUMERIC
            )
            
            logging.info(f"Data exported to CSV: {output_path} ({len(df)} records)")
            return True
            
        except Exception as e:
            logging.error(f"Error exporting to CSV: {e}", exc_info=True)
            return False
    
    @staticmethod
    def export_to_json(df: pd.DataFrame, output_path: str, 
                      indent: int = 2, orient: str = 'records') -> bool:
        """
        Экспорт данных в JSON формат
        
        Args:
            df: DataFrame с данными
            output_path: Путь для сохранения
            indent: Отступы для форматирования
            orient: Ориентация JSON ('records', 'index', 'columns')
        
        Returns:
            True если успешно, False иначе
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Конвертируем DataFrame в JSON
            json_data = df.to_dict(orient=orient)
            
            # Добавляем метаданные
            export_data = {
                "metadata": {
                    "export_date": datetime.now().isoformat(),
                    "total_records": len(df),
                    "format_version": "2.1.0"
                },
                "data": json_data
            }
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=indent, ensure_ascii=False)
            
            logging.info(f"Data exported to JSON: {output_path} ({len(df)} records)")
            return True
            
        except Exception as e:
            logging.error(f"Error exporting to JSON: {e}", exc_info=True)
            return False
    
    @staticmethod
    def export_to_markdown(df: pd.DataFrame, output_path: str, 
                          include_stats: bool = True) -> bool:
        """
        Экспорт данных в Markdown формат с таблицей
        
        Args:
            df: DataFrame с данными
            output_path: Путь для сохранения
            include_stats: Включать ли статистику
        
        Returns:
            True если успешно, False иначе
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                # Заголовок
                f.write("# Telegram Users Database Export\n\n")
                f.write(f"**Export Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Статистика
                if include_stats:
                    f.write("## Statistics\n\n")
                    f.write(f"- **Total Records:** {len(df)}\n")
                    
                    if 'Username' in df.columns:
                        with_username = df['Username'].notna().sum()
                        f.write(f"- **With Username:** {with_username} ({with_username/len(df)*100:.1f}%)\n")
                    
                    if 'Премиум' in df.columns:
                        premium = df['Премиум'].sum()
                        f.write(f"- **Premium Users:** {premium}\n")
                    
                    if 'Verified' in df.columns:
                        verified = df['Verified'].sum()
                        f.write(f"- **Verified Users:** {verified}\n")
                    
                    if 'Источник группы' in df.columns:
                        unique_groups = df['Источник группы'].nunique()
                        f.write(f"- **Unique Groups:** {unique_groups}\n")
                    
                    f.write("\n")
                
                # Таблица данных (первые 100 записей для производительности)
                f.write("## Data Sample (First 100 Records)\n\n")
                
                sample_df = df.head(100)
                f.write(sample_df.to_markdown(index=False))
                
                if len(df) > 100:
                    f.write(f"\n\n*... and {len(df) - 100} more records*\n")
            
            logging.info(f"Data exported to Markdown: {output_path}")
            return True
            
        except Exception as e:
            logging.error(f"Error exporting to Markdown: {e}", exc_info=True)
            return False
    
    @staticmethod
    def export_to_html(df: pd.DataFrame, output_path: str, 
                      title: str = "Telegram Users Database") -> bool:
        """
        Экспорт данных в HTML формат с стилями
        
        Args:
            df: DataFrame с данными
            output_path: Путь для сохранения
            title: Заголовок страницы
        
        Returns:
            True если успешно, False иначе
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # HTML шаблон
            html_template = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-card h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .stat-card p {{
            margin: 0;
            font-size: 32px;
            font-weight: bold;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            padding: 12px;
            text-align: left;
            position: sticky;
            top: 0;
        }}
        td {{
            padding: 10px;
            border-bottom: 1px solid #ddd;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .export-info {{
            margin-top: 20px;
            padding: 15px;
            background-color: #e8f5e9;
            border-left: 4px solid #4CAF50;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        
        <div class="export-info">
            <strong>Export Date:</strong> {export_date}<br>
            <strong>Total Records:</strong> {total_records:,}
        </div>
        
        <div class="stats">
            {stats_html}
        </div>
        
        {table_html}
    </div>
</body>
</html>
"""
            
            # Генерируем статистику
            stats_html = ""
            
            if 'Username' in df.columns:
                with_username = df['Username'].notna().sum()
                stats_html += f"""
                <div class="stat-card">
                    <h3>With Username</h3>
                    <p>{with_username:,}</p>
                </div>
                """
            
            if 'Премиум' in df.columns:
                premium = df['Премиум'].sum()
                stats_html += f"""
                <div class="stat-card">
                    <h3>Premium Users</h3>
                    <p>{premium:,}</p>
                </div>
                """
            
            if 'Источник группы' in df.columns:
                unique_groups = df['Источник группы'].nunique()
                stats_html += f"""
                <div class="stat-card">
                    <h3>Unique Groups</h3>
                    <p>{unique_groups:,}</p>
                </div>
                """
            
            # Генерируем таблицу
            table_html = df.head(1000).to_html(
                index=False,
                classes='data-table',
                border=0
            )
            
            # Формируем финальный HTML
            html_content = html_template.format(
                title=title,
                export_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                total_records=len(df),
                stats_html=stats_html,
                table_html=table_html
            )
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logging.info(f"Data exported to HTML: {output_path}")
            return True
            
        except Exception as e:
            logging.error(f"Error exporting to HTML: {e}", exc_info=True)
            return False
    
    @staticmethod
    def create_text_report(stats: Dict[str, Any], output_path: str) -> bool:
        """
        Создание текстового отчета со статистикой
        
        Args:
            stats: Словарь со статистикой
            output_path: Путь для сохранения
        
        Returns:
            True если успешно, False иначе
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("=" * 60 + "\n")
                f.write("TELEGRAM USERS DATABASE REPORT\n")
                f.write("=" * 60 + "\n\n")
                
                f.write(f"Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("GENERAL STATISTICS\n")
                f.write("-" * 60 + "\n")
                f.write(f"Total Users:           {stats.get('total_users', 0):>15,}\n")
                f.write(f"Unique Users:          {stats.get('unique_users', 0):>15,}\n")
                f.write(f"With Username:         {stats.get('with_username', 0):>15,}\n")
                f.write(f"Premium Users:         {stats.get('premium_users', 0):>15,}\n")
                f.write(f"Verified Users:        {stats.get('verified_users', 0):>15,}\n")
                
                if 'bot_accounts' in stats:
                    f.write(f"Bot Accounts:          {stats['bot_accounts']:>15,}\n")
                
                f.write("\n")
                
                if 'first_record' in stats:
                    f.write("TIME PERIOD\n")
                    f.write("-" * 60 + "\n")
                    f.write(f"First Record:          {stats['first_record']}\n")
                    f.write(f"Last Record:           {stats['last_record']}\n\n")
                
                if 'most_active_day' in stats:
                    f.write("ACTIVITY\n")
                    f.write("-" * 60 + "\n")
                    f.write(f"Most Active Day:       {stats['most_active_day']}\n")
                    f.write(f"Users on That Day:     {stats['most_active_day_count']:,}\n\n")
                
                if 'top_sources' in stats and stats['top_sources']:
                    f.write("TOP SOURCES (Groups)\n")
                    f.write("-" * 60 + "\n")
                    for idx, (source, count) in enumerate(stats['top_sources'].items(), 1):
                        f.write(f"{idx}. {source:<40} {count:>10,}\n")
                    f.write("\n")
                
                f.write("=" * 60 + "\n")
                f.write("End of Report\n")
                f.write("=" * 60 + "\n")
            
            logging.info(f"Text report created: {output_path}")
            return True
            
        except Exception as e:
            logging.error(f"Error creating text report: {e}", exc_info=True)
            return False
    
    @staticmethod
    def export_all_formats(df: pd.DataFrame, stats: Dict[str, Any], 
                          base_path: str, prefix: str = "export") -> Dict[str, str]:
        """
        Экспорт во все поддерживаемые форматы
        
        Args:
            df: DataFrame с данными
            stats: Словарь со статистикой
            base_path: Базовый путь для экспорта
            prefix: Префикс имени файла
        
        Returns:
            Словарь с путями к созданным файлам
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results = {}
        
        # Excel
        excel_path = os.path.join(base_path, f"{prefix}_{timestamp}.xlsx")
        try:
            from ..utils.database import DatabaseManager
            excel_result = DatabaseManager.export_to_excel(excel_path)
            if excel_result:
                results['excel'] = excel_result
        except Exception as e:
            logging.error(f"Excel export failed: {e}")
        
        # CSV
        csv_path = os.path.join(base_path, f"{prefix}_{timestamp}.csv")
        if AdvancedExporter.export_to_csv(df, csv_path):
            results['csv'] = csv_path
        
        # JSON
        json_path = os.path.join(base_path, f"{prefix}_{timestamp}.json")
        if AdvancedExporter.export_to_json(df, json_path):
            results['json'] = json_path
        
        # Markdown
        md_path = os.path.join(base_path, f"{prefix}_{timestamp}.md")
        if AdvancedExporter.export_to_markdown(df, md_path):
            results['markdown'] = md_path
        
        # HTML
        html_path = os.path.join(base_path, f"{prefix}_{timestamp}.html")
        if AdvancedExporter.export_to_html(df, html_path):
            results['html'] = html_path
        
        # Text Report
        txt_path = os.path.join(base_path, f"{prefix}_report_{timestamp}.txt")
        if AdvancedExporter.create_text_report(stats, txt_path):
            results['report'] = txt_path
        
        logging.info(f"Exported to {len(results)} formats: {list(results.keys())}")
        return results

