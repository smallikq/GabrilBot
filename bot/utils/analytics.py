import logging
import pandas as pd
from typing import Dict, Any


class Analytics:
    """Класс для аналитики и статистики"""

    @staticmethod
    def get_enhanced_analytics(df: pd.DataFrame) -> Dict[str, Any]:
        """Получение расширенной аналитики"""
        try:
            analytics_data = {
                "total_users": len(df),
                "with_username": df["Username"].notna().sum(),
                "without_username": df["Username"].isna().sum(),
                "premium_users": df["Премиум"].sum() if "Премиум" in df.columns else 0,
                "verified_users": df["Verified"].sum() if "Verified" in df.columns else 0,
            }

            # Анализ активности по времени
            if "Время сбора (UTC+1)" in df.columns:
                df["Время сбора (UTC+1)"] = pd.to_datetime(df["Время сбора (UTC+1)"], errors="coerce")
                df_time = df.dropna(subset=["Время сбора (UTC+1)"])

                if not df_time.empty:
                    # Статистика по дням недели
                    df_time['day_of_week'] = df_time["Время сбора (UTC+1)"].dt.day_name()
                    day_stats = df_time['day_of_week'].value_counts()
                    analytics_data["day_of_week_stats"] = day_stats.head(3).to_dict()

                    # Статистика по часам
                    df_time['hour'] = df_time["Время сбора (UTC+1)"].dt.hour
                    hour_stats = df_time['hour'].value_counts().sort_index()
                    peak_hour = hour_stats.idxmax()
                    analytics_data["peak_hour"] = peak_hour
                    analytics_data["peak_hour_count"] = hour_stats[peak_hour]

                    # Прогноз роста
                    daily_growth = df_time.groupby(df_time["Время сбора (UTC+1)"].dt.date).size()
                    if len(daily_growth) > 1:
                        avg_daily = daily_growth.mean()
                        analytics_data["avg_daily_growth"] = avg_daily
                        analytics_data["weekly_forecast"] = avg_daily * 7

            # Анализ источников
            if "Источник группы" in df.columns:
                source_stats = df["Источник группы"].value_counts()
                analytics_data["total_groups"] = len(source_stats)
                analytics_data["top_group"] = source_stats.index[0]
                analytics_data["top_group_count"] = source_stats.iloc[0]

            return analytics_data

        except Exception as e:
            logging.error(f"Error in analytics: {e}")
            return {"error": str(e)}

    @staticmethod
    def format_analytics_text(analytics_data: Dict[str, Any]) -> str:
        """Форматирование текста аналитики для отправки"""
        if "error" in analytics_data:
            return f"❌ Ошибка аналитики: {analytics_data['error']}"

        text = "📈 <b>Детальная аналитика</b>\n\n"

        # Основная статистика
        text += f"👥 <b>Всего пользователей:</b> {analytics_data['total_users']:,}\n"
        text += f"🏷 <b>С username:</b> {analytics_data['with_username']:,} ({analytics_data['with_username'] / analytics_data['total_users'] * 100:.1f}%)\n"
        text += f"🏷 <b>Без username:</b> {analytics_data['without_username']:,} ({analytics_data['without_username'] / analytics_data['total_users'] * 100:.1f}%)\n"

        if analytics_data.get('premium_users', 0) > 0:
            text += f"💎 <b>Premium:</b> {analytics_data['premium_users']:,}\n"

        if analytics_data.get('verified_users', 0) > 0:
            text += f"✅ <b>Verified:</b> {analytics_data['verified_users']:,}\n"

        # Активность по дням недели
        if "day_of_week_stats" in analytics_data:
            text += "\n📅 <b>Активность по дням недели:</b>\n"
            for day, count in analytics_data["day_of_week_stats"].items():
                text += f"• {day}: {count} пользователей\n"

        # Пиковый час
        if "peak_hour" in analytics_data:
            text += f"\n🕐 <b>Пиковый час активности:</b> {analytics_data['peak_hour']}:00 ({analytics_data['peak_hour_count']} пользователей)\n"

        # Анализ источников
        if "total_groups" in analytics_data:
            text += f"\n🎯 <b>Анализ источников:</b>\n"
            text += f"• Всего уникальных групп: {analytics_data['total_groups']}\n"
            text += f"• Самая продуктивная группа: {analytics_data['top_group']} ({analytics_data['top_group_count']} пользователей)\n"

        # Прогнозы
        if "avg_daily_growth" in analytics_data:
            text += f"\n📊 <b>Прогнозы:</b>\n"
            text += f"• Среднесуточный рост: {analytics_data['avg_daily_growth']:.1f} пользователей\n"
            text += f"• Прогноз на неделю: +{analytics_data['weekly_forecast']:.0f} пользователей\n"

        return text

