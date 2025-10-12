import os
import logging
from aiogram import types, F

from ..utils.database import DatabaseManager
from ..aiogram_loader import dp


@dp.message(F.text == "📊 Статистика и Аналитика")
async def show_combined_stats(message: types.Message):
    """Объединенная статистика и аналитика"""
    try:
        import pandas as pd
        from ..utils.analytics import Analytics
        from ..keyboards.settings_menu import get_combined_stats_keyboard
        
        # Получаем базовую статистику
        stats = DatabaseManager.get_database_stats()
        
        if 'error' in stats:
            await message.answer(f"❌ Ошибка: {stats['error']}")
            return
        
        if stats['total_users'] == 0:
            await message.answer("📊 База данных пуста")
            return
        
        # Получаем данные для аналитики
        df = DatabaseManager.get_all_users()
        
        # Формируем объединенный текст
        combined_text = f"📊 <b>Статистика и Аналитика</b>\n\n"
        
        # Основная статистика
        combined_text += f"<b>═══ ОСНОВНАЯ СТАТИСТИКА ═══</b>\n"
        combined_text += f"👥 <b>Всего пользователей:</b> {stats['total_users']:,}\n"
        combined_text += f"🏷 <b>С username:</b> {stats['with_username']:,} ({stats['with_username'] / stats['total_users'] * 100:.1f}%)\n"
        combined_text += f"🏷 <b>Без username:</b> {stats['total_users'] - stats['with_username']:,} ({(stats['total_users'] - stats['with_username']) / stats['total_users'] * 100:.1f}%)\n"
        
        if stats.get('premium_users', 0) > 0:
            combined_text += f"💎 <b>Premium:</b> {stats['premium_users']:,}\n"
        
        if stats.get('verified_users', 0) > 0:
            combined_text += f"✅ <b>Verified:</b> {stats['verified_users']:,}\n"
        
        # Период сбора
        if 'first_record' in stats and 'last_record' in stats:
            combined_text += f"\n<b>═══ ПЕРИОД СБОРА ═══</b>\n"
            combined_text += f"📅 Первая запись: {stats['first_record'].strftime('%d.%m.%Y')}\n"
            combined_text += f"📅 Последняя запись: {stats['last_record'].strftime('%d.%m.%Y')}\n"
        
        if 'most_active_day' in stats:
            combined_text += f"⭐ Самый активный день: {stats['most_active_day']} ({stats['most_active_day_count']} польз.)\n"
        
        # Расширенная аналитика
        if not df.empty:
            analytics_data = Analytics.get_enhanced_analytics(df)
            
            # Анализ активности
            if "peak_hour" in analytics_data:
                combined_text += f"\n<b>═══ АНАЛИЗ АКТИВНОСТИ ═══</b>\n"
                combined_text += f"🕐 Пиковый час: {analytics_data['peak_hour']}:00 ({analytics_data['peak_hour_count']} польз.)\n"
            
            # Активность по дням недели
            if "day_of_week_stats" in analytics_data:
                combined_text += f"📅 <b>Топ-3 дня недели:</b>\n"
                for i, (day, count) in enumerate(list(analytics_data["day_of_week_stats"].items())[:3], 1):
                    combined_text += f"  {i}. {day}: {count} польз.\n"
            
            # Прогнозы
            if "avg_daily_growth" in analytics_data:
                combined_text += f"\n<b>═══ ПРОГНОЗЫ РОСТА ═══</b>\n"
                combined_text += f"📈 Среднесуточный рост: {analytics_data['avg_daily_growth']:.1f} польз.\n"
                combined_text += f"📊 Прогноз на неделю: +{analytics_data['weekly_forecast']:.0f} польз.\n"
        
        # Топ источников
        if 'top_sources' in stats:
            combined_text += f"\n<b>═══ ТОП-5 ИСТОЧНИКОВ ═══</b>\n"
            for i, (source, count) in enumerate(list(stats['top_sources'].items())[:5], 1):
                combined_text += f"{i}. {source[:35]}: {count}\n"
        
        await message.answer(combined_text, reply_markup=get_combined_stats_keyboard(), parse_mode="HTML")
    
    except Exception as e:
        logging.error(f"Error in combined stats: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка при создании статистики: {e}")


@dp.callback_query(F.data == "refresh_stats")
async def refresh_stats(callback_query: types.CallbackQuery):
    """Обновление статистики"""
    await callback_query.answer("Обновляю статистику...")
    await show_combined_stats(callback_query.message)


@dp.callback_query(F.data == "create_analytics_report")
async def create_analytics_report(callback_query: types.CallbackQuery):
    """Создание детального отчета аналитики"""
    await callback_query.answer("Создаю отчет...")
    
    try:
        import os
        from ..utils.analytics import Analytics
        from ..aiogram_loader import bot
        from aiogram.types import FSInputFile
        from datetime import datetime
        import pandas as pd
        
        df = DatabaseManager.get_all_users()
        
        if df.empty:
            await bot.send_message(callback_query.message.chat.id, "❌ Нет данных для отчета")
            return
        
        # Создаем детальный отчет в виде HTML
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f'bot/data/exports/analytics_report_{timestamp}.html'
        
        analytics_data = Analytics.get_enhanced_analytics(df)
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Отчет Аналитики</title>
            <style>
                body {{ font-family: Arial, sans-serif; padding: 20px; background: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; }}
                h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
                h2 {{ color: #34495e; margin-top: 30px; }}
                .stat-box {{ display: inline-block; background: #ecf0f1; padding: 15px 25px; margin: 10px; border-radius: 5px; }}
                .stat-value {{ font-size: 28px; font-weight: bold; color: #3498db; }}
                .stat-label {{ font-size: 14px; color: #7f8c8d; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #3498db; color: white; }}
                tr:hover {{ background-color: #f5f5f5; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📊 Отчет Аналитики Telegram Parser Bot</h1>
                <p><strong>Дата создания:</strong> {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}</p>
                
                <h2>Основная Статистика</h2>
                <div class="stat-box">
                    <div class="stat-value">{analytics_data['total_users']:,}</div>
                    <div class="stat-label">Всего пользователей</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">{analytics_data['with_username']:,}</div>
                    <div class="stat-label">С username</div>
                </div>
                <div class="stat-box">
                    <div class="stat-value">{analytics_data.get('premium_users', 0):,}</div>
                    <div class="stat-label">Premium</div>
                </div>
                
                <h2>Процентное Соотношение</h2>
                <table>
                    <tr>
                        <th>Метрика</th>
                        <th>Значение</th>
                        <th>Процент</th>
                    </tr>
                    <tr>
                        <td>С username</td>
                        <td>{analytics_data['with_username']:,}</td>
                        <td>{analytics_data['with_username'] / analytics_data['total_users'] * 100:.1f}%</td>
                    </tr>
                    <tr>
                        <td>Без username</td>
                        <td>{analytics_data['without_username']:,}</td>
                        <td>{analytics_data['without_username'] / analytics_data['total_users'] * 100:.1f}%</td>
                    </tr>
        """
        
        if analytics_data.get('premium_users', 0) > 0:
            html_content += f"""
                    <tr>
                        <td>Premium пользователи</td>
                        <td>{analytics_data['premium_users']:,}</td>
                        <td>{analytics_data['premium_users'] / analytics_data['total_users'] * 100:.1f}%</td>
                    </tr>
            """
        
        if "total_groups" in analytics_data:
            html_content += f"""
                </table>
                <h2>Анализ Источников</h2>
                <p><strong>Всего уникальных групп:</strong> {analytics_data['total_groups']}</p>
                <p><strong>Самая продуктивная группа:</strong> {analytics_data['top_group']} ({analytics_data['top_group_count']} пользователей)</p>
            """
        
        html_content += """
            </div>
        </body>
        </html>
        """
        
        os.makedirs('bot/data/exports', exist_ok=True)
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        await bot.send_document(
            callback_query.message.chat.id,
            FSInputFile(report_path, filename=f'analytics_report_{timestamp}.html'),
            caption="📊 Детальный отчет аналитики создан"
        )
        
    except Exception as e:
        logging.error(f"Error creating analytics report: {e}", exc_info=True)
        from ..aiogram_loader import bot
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка создания отчета: {e}")


@dp.callback_query(F.data == "export_stats")
async def export_stats_callback(callback_query: types.CallbackQuery):
    """Экспорт статистики"""
    await callback_query.answer("Экспортирую статистику...")
    
    try:
        from ..aiogram_loader import bot
        from aiogram.types import FSInputFile
        from datetime import datetime
        
        stats = DatabaseManager.get_database_stats()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Создаем CSV файл со статистикой
        stats_path = f'bot/data/exports/stats_{timestamp}.csv'
        os.makedirs('bot/data/exports', exist_ok=True)
        
        with open(stats_path, 'w', encoding='utf-8') as f:
            f.write("Метрика,Значение\n")
            f.write(f"Всего пользователей,{stats.get('total_users', 0)}\n")
            f.write(f"С username,{stats.get('with_username', 0)}\n")
            f.write(f"Premium,{stats.get('premium_users', 0)}\n")
            f.write(f"Verified,{stats.get('verified_users', 0)}\n")
            if 'first_record' in stats:
                f.write(f"Первая запись,{stats['first_record'].strftime('%d.%m.%Y')}\n")
            if 'last_record' in stats:
                f.write(f"Последняя запись,{stats['last_record'].strftime('%d.%m.%Y')}\n")
        
        await bot.send_document(
            callback_query.message.chat.id,
            FSInputFile(stats_path, filename=f'stats_{timestamp}.csv'),
            caption="📊 Экспорт статистики"
        )
        
    except Exception as e:
        logging.error(f"Error exporting stats: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка экспорта: {e}")


@dp.callback_query(F.data == "create_charts")
async def create_charts(callback_query: types.CallbackQuery):
    """Создание графиков"""
    await callback_query.answer("Создаю графики...")
    
    try:
        import os
        from ..aiogram_loader import bot
        from aiogram.types import FSInputFile
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import pandas as pd
        from datetime import datetime
        
        df = DatabaseManager.get_all_users()
        
        if df.empty:
            await bot.send_message(callback_query.message.chat.id, "❌ Нет данных для графиков")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        chart_path = f'bot/data/exports/charts_{timestamp}.png'
        os.makedirs('bot/data/exports', exist_ok=True)
        
        # Создаем несколько графиков
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Аналитика пользователей', fontsize=16, fontweight='bold')
        
        # График 1: Username vs No Username
        username_counts = [df["Username"].notna().sum(), df["Username"].isna().sum()]
        axes[0, 0].pie(username_counts, labels=['С username', 'Без username'], autopct='%1.1f%%', colors=['#3498db', '#e74c3c'])
        axes[0, 0].set_title('Распределение по username')
        
        # График 2: Premium пользователи
        if "Премиум" in df.columns:
            premium_counts = df["Премиум"].value_counts()
            axes[0, 1].bar(['Обычные', 'Premium'], [len(df) - premium_counts.get(True, 0), premium_counts.get(True, 0)], color=['#95a5a6', '#f39c12'])
            axes[0, 1].set_title('Premium пользователи')
            axes[0, 1].set_ylabel('Количество')
        
        # График 3: Топ-10 источников
        if "Источник группы" in df.columns:
            top_sources = df["Источник группы"].value_counts().head(10)
            axes[1, 0].barh(range(len(top_sources)), top_sources.values, color='#16a085')
            axes[1, 0].set_yticks(range(len(top_sources)))
            axes[1, 0].set_yticklabels([s[:30] for s in top_sources.index], fontsize=8)
            axes[1, 0].set_title('Топ-10 источников')
            axes[1, 0].set_xlabel('Количество пользователей')
        
        # График 4: Активность по дням (если есть данные времени)
        if "Время сбора (UTC+1)" in df.columns:
            df["Время сбора (UTC+1)"] = pd.to_datetime(df["Время сбора (UTC+1)"], errors="coerce")
            df_time = df.dropna(subset=["Время сбора (UTC+1)"])
            if not df_time.empty:
                daily_counts = df_time.groupby(df_time["Время сбора (UTC+1)"].dt.date).size()
                axes[1, 1].plot(daily_counts.index, daily_counts.values, marker='o', color='#9b59b6', linewidth=2)
                axes[1, 1].set_title('Активность по дням')
                axes[1, 1].set_xlabel('Дата')
                axes[1, 1].set_ylabel('Количество пользователей')
                axes[1, 1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(chart_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        await bot.send_photo(
            callback_query.message.chat.id,
            FSInputFile(chart_path),
            caption="📈 Графики аналитики"
        )
        
        # Удаляем временный файл
        try:
            os.remove(chart_path)
        except:
            pass
        
    except Exception as e:
        logging.error(f"Error creating charts: {e}", exc_info=True)
        from ..aiogram_loader import bot
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка создания графиков: {e}")



