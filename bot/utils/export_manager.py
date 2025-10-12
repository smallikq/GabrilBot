import os
import tempfile
import zipfile
import logging
from datetime import datetime
import pandas as pd


class ExportManager:
    """Менеджер экспорта данных в различные форматы"""

    @staticmethod
    async def export_to_csv(df, filename):
        """Экспорт в CSV"""
        try:
            csv_path = f'bot/data/exports/{filename}.csv'
            df.to_csv(csv_path, index=False, encoding='utf-8')
            return csv_path
        except Exception as e:
            logging.error(f"Error exporting to CSV: {e}")
            return None

    @staticmethod
    async def export_to_json(df, filename):
        """Экспорт в JSON"""
        try:
            json_path = f'bot/data/exports/{filename}.json'
            df.to_json(json_path, orient='records', force_ascii=False, indent=2)
            return json_path
        except Exception as e:
            logging.error(f"Error exporting to JSON: {e}")
            return None

    @staticmethod
    async def create_report(df, filename):
        """Создание детального отчета"""
        try:
            report_path = f'bot/data/exports/{filename}_report.txt'

            stats = {
                "Общее количество пользователей": len(df),
                "С username": df["Username"].notna().sum(),
                "С именем": df["Имя"].notna().sum(),
                "Premium пользователи": df["Премиум"].sum() if "Премиум" in df.columns else 0,
            }

            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("=== ОТЧЕТ ПО БАЗЕ ДАННЫХ ===\n\n")
                f.write(f"Дата создания отчета: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                f.write("ОСНОВНАЯ СТАТИСТИКА:\n")
                for key, value in stats.items():
                    f.write(f"• {key}: {value}\n")

                f.write(f"\n• Процент с username: {(stats['С username'] / len(df) * 100):.1f}%\n")

                if "Источник группы" in df.columns:
                    f.write("\nТОП-10 ИСТОЧНИКОВ:\n")
                    source_stats = df["Источник группы"].value_counts().head(10)
                    for source, count in source_stats.items():
                        f.write(f"• {source}: {count} пользователей\n")

                if "Время сбора (UTC+1)" in df.columns:
                    df["Время сбора (UTC+1)"] = pd.to_datetime(df["Время сбора (UTC+1)"], errors="coerce")
                    df_clean = df.dropna(subset=["Время сбора (UTC+1)"])
                    if not df_clean.empty:
                        daily_stats = df_clean.groupby(df_clean["Время сбора (UTC+1)"].dt.date).size()
                        f.write(f"\nСТАТИСТИКА ПО ДНЯМ:\n")
                        f.write(f"• Период: с {daily_stats.index.min()} по {daily_stats.index.max()}\n")
                        f.write(f"• Самый активный день: {daily_stats.idxmax()} ({daily_stats.max()} пользователей)\n")
                        f.write(f"• Среднее в день: {daily_stats.mean():.1f} пользователей\n")

            return report_path
        except Exception as e:
            logging.error(f"Error creating report: {e}")
            return None

    @staticmethod
    async def create_complete_export(df, filename):
        """Создание полного экспорта со всеми форматами"""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Создаем все форматы
                excel_path = os.path.join(temp_dir, f'{filename}.xlsx')
                csv_path = os.path.join(temp_dir, f'{filename}.csv')
                json_path = os.path.join(temp_dir, f'{filename}.json')
                report_path = os.path.join(temp_dir, f'report_{filename}.txt')

                df.to_excel(excel_path, index=False)
                df.to_csv(csv_path, index=False, encoding='utf-8')
                df.to_json(json_path, orient='records', force_ascii=False, indent=2)

                # Создаем архив
                file_path = f'bot/data/exports/complete_export_{filename}.zip'
                with zipfile.ZipFile(file_path, 'w') as zipf:
                    zipf.write(excel_path, f'{filename}.xlsx')
                    zipf.write(csv_path, f'{filename}.csv')
                    zipf.write(json_path, f'{filename}.json')

                    # Добавляем отчет
                    report_content = f"Экспорт базы данных\n"
                    report_content += f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    report_content += f"Всего пользователей: {len(df)}\n"
                    report_content += f"Форматы: Excel, CSV, JSON\n"

                    with open(report_path, 'w', encoding='utf-8') as f:
                        f.write(report_content)
                    zipf.write(report_path, f'readme_{filename}.txt')

            return file_path
        except Exception as e:
            logging.error(f"Error creating complete export: {e}")
            return None

