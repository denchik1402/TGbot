import os 
import sqlite3
from datetime import datetime, timedelta
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackContext,
)
import logging
import pytz  # Для работы с часовыми поясами
from pytz import timezone as pytz_timezone

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Список администраторов
ADMINS = ["user1"] #Вставь юзернейм администраторов, например ["user1", "user2"]
# Кодовые слова для уровней
LEVEL_CODES = {
    "Ты пока не обучаешься на курсе! Приступай скорее!": "❌ Ты пока не обучаешься на курсе! Приступай скорее! ❌",
    "Стремление к знаниям": "🤝Добро пожаловать на курс!🤝",
    "Усердие": "👍Защитил первый блок!👍",
    "Интеллект": "✊Защитил второй блок!✊",
    "Прогресс": "💪Защитил третий блок!💪",
    "Умение учиться": "🔥Защитил четвёртый блок!🔥",
    "Отличные результаты": "🎉Защитил пятый блок!🎉",
    "Трудолюбие": "🥳Защитил финальное задание!🥳",
    "Академические достижения": "🎉Прошёл тестовое собеседование!🎉",
    "Я ОЙТИШНИК!": "🦸‍♂️Коллега!🦸‍♂️"
}

# Поощрения для уровней
LEVEL_REWARDS = {
    "🤝Добро пожаловать на курс!🤝": "🤝Отличная работа!🤝 \n😺Ты сделал первый шаг к успеху!😺",
    "👍Защитил первый блок!👍": "😎Ты крут!😎 \n😏Продолжай развиваться!😏",
    "✊Защитил второй блок!✊": "✊Ты движешься вперёд!✊ \n💪Не останачивайся!💪",
    "💪Защитил третий блок!💪": "💥Замечательно!💥 \n🏅Ты показываешь отличные результаты!🏅",
    "🔥Защитил четвёртый блок!🔥": "✔️Ты на правильном пути!✔️ \n🦾Продолжай в том же духе!🦾",
    "🎉Защитил пятый блок!🎉": "😃Прекрасно!😃 \n⭐️Ты достиг нового уровня!⭐️",
    "🥳Защитил финальное задание!🥳": "🙀Ты уже почти на финише!🙀 \n💪Продолжай в том же духе!💪",
    "🎉Прошёл тестовое собеседование!🎉": "🎊Поздравляю!🎊 \n🎉Ты настоящий профессионал!🎉",
    "🦸‍♂️Коллега!🦸‍♂️": "🎊Поздравляю!🎊 \n🎉Ты настоящий профессионал!🎉"
}

LEVEL_ORDER = [
    "❌ Ты пока не обучаешься на курсе! Приступай скорее! ❌",
    "🤝Добро пожаловать на курс!🤝",
    "👍Защитил первый блок!👍",
    "✊Защитил второй блок!✊",
    "💪Защитил третий блок!💪",
    "🔥Защитил четвёртый блок!🔥",
    "🎉Защитил пятый блок!🎉",
    "🥳Защитил финальное задание!🥳",
    "🎉Прошёл тестовое собеседование!🎉",
    "🦸‍♂️Коллега!🦸‍♂️"
]

# Установка московского времени по умолчанию
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Глобальный флаг для отслеживания инициализации базы данных
database_initialized = False

# Создание базы данных, если её нет
def create_database():
    conn = sqlite3.connect('bot_database.db')
    cursor = conn.cursor()

    # Создаем таблицу users, если её нет
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE,
            full_name TEXT,
            is_admin BOOLEAN DEFAULT FALSE,
            level TEXT DEFAULT '❌ Ты пока не обучаешься на курсе! Приступай скорее! ❌',
            joined_at TEXT,
            status TEXT DEFAULT 'Интересующийся',
            is_blocked BOOLEAN DEFAULT FALSE,
            chat_id INTEGER UNIQUE
        );
    ''')

    # Создаем таблицу schedule, если её нет
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS schedule (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            event_name TEXT,
            event_date TEXT,
            event_time TEXT,
            is_blocked_day BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
    ''')

    # Создаем таблицу progress, если её нет
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS progress (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            level TEXT,
            completed_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
    ''')

    # Создаем таблицу reminders, если её нет
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            reminder_type TEXT,
            reminder_time TEXT,  -- Храним время в формате YYYY-MM-DD HH:MM
            reminder_text TEXT,
            last_sent_date TEXT,  -- Дата последней отправки напоминания
            user_meeting_reminder_minutes INTEGER,
            meeting_reminder_minutes INTEGER,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
    ''')

    # Создаем таблицу reminders, если её нет
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sent_reminders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            event_date TEXT,
            event_time TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
    ''')

    # Создаем таблицу questions, если её нет
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS questions (
            id INTEGER PRIMARY KEY,
            block INTEGER,
            question_text TEXT UNIQUE,
            option1 TEXT,
            option2 TEXT,
            option3 TEXT,
            option4 TEXT,
            correct_option INTEGER
        );
    ''')

    conn.commit()
    conn.close()
    

    # Устанавливаем флаг, что база данных инициализирована
    database_initialized = True

# Функция для подключения к базе данных
def get_db_connection():
    if not os.path.exists('bot_database.db'):
        create_database()
    conn = sqlite3.connect('bot_database.db')
    conn.row_factory = sqlite3.Row
    return conn

# Проверка, является ли пользователь администратором
def is_admin(username):
    return username in ADMINS
    
def get_moscow_time():
    return datetime.now(MOSCOW_TZ)
    logger.warning("Время Московское")

# Тайм-аут состояния (5 минут)
STATE_TIMEOUT = timedelta(minutes=5)

# Обновляем время последней активности
def update_last_active(context: CallbackContext):
    context.user_data['last_active'] = datetime.now().isoformat()

# Проверка тайм-аута состояния
async def check_state_timeout(context: CallbackContext):
    logger.info("Запуск задачи check_state_timeout")
    try:
        if context.job is None or context.job.data is None:
            logger.warning("context.job или context.job.data равен None. Задача завершена.")
            return

        user_data = context.job.data.get("user_data")
        if user_data is None:
            logger.warning("user_data равен None. Задача завершена.")
            return

        if 'last_active' not in user_data:
            logger.warning("Ключ 'last_active' отсутствует в user_data")
            return

        last_active = datetime.fromisoformat(user_data['last_active'])
        logger.info(f"Последняя активность: {last_active}")

        if datetime.now() - last_active > STATE_TIMEOUT:
            logger.info("Тайм-аут состояния: очистка данных")
            user_data.clear()
            await context.bot.send_message(
                chat_id=user_data.get('user_id'),
                text="⏳ Время ожидания истекло. Начните заново.😊"
            )
            
    except Exception as e:
        logger.error(f"Ошибка в check_state_timeout: {e}", exc_info=True)

async def register_admins(context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    for admin in ADMINS:
        # Проверяем, существует ли администратор в базе данных
        cursor.execute('SELECT id FROM users WHERE username = ?', (admin,))
        admin_data = cursor.fetchone()
        if admin_data:
            # Если администратор существует, обновляем его chat_id
            cursor.execute('UPDATE users SET chat_id = ? WHERE username = ?', (admin_data['id'], admin))
        else:
            # Если администратора нет, добавляем его
            cursor.execute('INSERT INTO users (username, is_admin) VALUES (?, ?)', (admin, True))
    conn.commit()
    conn.close()


# Основное меню
async def start(update: Update, context: CallbackContext):
    # Создаем локальное соединение с базой данных
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Очищаем стек при возврате в главное меню
        if 'menu_stack' in context.user_data:
            context.user_data['menu_stack'].clear()

        # Инициализируем переменную user
        user = update.effective_user
        logger.info(f"User {user.username} started the bot.")

        # Обновляем chat_id администратора
        if is_admin(user.username):
            cursor.execute("UPDATE users SET chat_id = ? WHERE username = ?", (user.id, user.username))
            conn.commit()
            logger.info(f"Обновлен chat_id для администратора {user.username} (chat_id: {user.id})")

        # Проверяем, есть ли ФИО пользователя в базе данных
        cursor.execute("SELECT full_name FROM users WHERE id = ?", (user.id,))
        full_name = cursor.fetchone()
        logger.info(f"Fetched full_name: {full_name}")

        if not full_name or not full_name[0]:
            logger.info("No full_name found, requesting registration.")
            await update.effective_message.reply_text("👋 Привет! Давай познакомимся! \n🤝 Введи своё имя в формате: Фамилия Имя. \n\nНапример: Иванов Иван.")
            context.user_data['step'] = 'get_full_name'
            context.user_data['user_id'] = user.id  # Сохраняем ID пользователя
            update_last_active(context)  # Обновляем время последней активности
            return

        logger.info("Full_name found, showing main menu.")
        keyboard = [
            [InlineKeyboardButton("📅 Расписание", callback_data='schedule'), InlineKeyboardButton("📊 Прогресс", callback_data='progress')],
            [InlineKeyboardButton("🎓 Тестирование", callback_data='testing'),InlineKeyboardButton("🆘 Получить помощь", callback_data='help')]
        ]
        if is_admin(user.username):
            keyboard.append([InlineKeyboardButton("👑 Админ", callback_data='admin')])
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Редактируем текущее сообщение или отправляем новое
        if update.callback_query:
            await update.callback_query.edit_message_text(text='📋 Выбери действие из меню ниже: \n\nP.S.\n"Если хочешь остановиться  в любой момент, а затем вернуться в меню введи: "STOP"', reply_markup=reply_markup)
        else:
            await update.effective_message.reply_text('👋 Выбирай чем хочешь воспользоваться:', reply_markup=reply_markup)

        context.user_data['user_id'] = user.id  # Сохраняем ID пользователя
        update_last_active(context)  # Обновляем время последней активности

    except Exception as e:
        logger.error(f"Ошибка в start: {e}", exc_info=True)

    finally:
        if conn:
            conn.close()

async def stop_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Универсальный обработчик для остановки выполнения и возврата в основное меню."""
    # Очищаем данные контекста
    context.user_data.clear()
    
    # Отправляем основное меню
    await start(update, context)

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /stop."""
    await stop_handler(update, context)

# Меню расписания
async def schedule_menu(update: Update, context: CallbackContext):
    # Добавляем текущее меню в стек, если его там еще нет
    if 'menu_stack' not in context.user_data:
        context.user_data['menu_stack'] = []
    
    # Проверяем, что текущее меню еще не в стеке
    if not context.user_data['menu_stack'] or context.user_data['menu_stack'][-1] != schedule_menu:
        context.user_data['menu_stack'].append(schedule_menu)
        logger.info(f"Добавлено меню 'schedule_menu' в стек. Текущий стек: {[func.__name__ for func in context.user_data['menu_stack']]}")

    # Создаем клавиатуру для меню расписания
    keyboard = [
        [InlineKeyboardButton("👀 Посмотреть расписание", callback_data='view_schedule'), InlineKeyboardButton("✅ Назначить встречу", callback_data='add_event')],
        [InlineKeyboardButton("📅 Мои встречи", callback_data='my_events'), InlineKeyboardButton("🔄 Перенести встречу", callback_data='reschedule_event')],
        [InlineKeyboardButton("⏪ Прошедшие встречи", callback_data='past_events'), InlineKeyboardButton("❌ Удалить встречу", callback_data='delete_event')],
        [InlineKeyboardButton("🔔 Напоминания пользователей", callback_data='user_reminders')],  # Новая кнопка
        [InlineKeyboardButton("🔙 Назад", callback_data='back')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Редактируем текущее сообщение
    await update.callback_query.edit_message_text(text="📅 Меню расписания:", reply_markup=reply_markup)
    update_last_active(context)  # Обновляем время последней активности

# Меню прогресса
async def progress_menu(update: Update, context: CallbackContext):
    # Добавляем текущее меню в стек, если его там еще нет
    if 'menu_stack' not in context.user_data:
        context.user_data['menu_stack'] = []
    
    # Проверяем, что текущее меню еще не в стеке
    if not context.user_data['menu_stack'] or context.user_data['menu_stack'][-1] != progress_menu:
        context.user_data['menu_stack'].append(progress_menu)
        logger.info(f"Добавлено меню 'progress_menu' в стек. Текущий стек: {[func.__name__ for func in context.user_data['menu_stack']]}")

    keyboard = [
        [InlineKeyboardButton("📈 Посмотреть прогресс", callback_data='view_progress')],
        [InlineKeyboardButton("🚀 Повысить уровень", callback_data='level_up')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query = update.callback_query

    # Проверяем, изменились ли текст или разметка
    current_text = "Меню расписания:"
    if query.message.text != current_text or query.message.reply_markup != reply_markup:
        await query.edit_message_text(text=current_text, reply_markup=reply_markup)
    else:
        logger.info("🔍 Сообщение не изменилось. Пропускаем обновление. 😉")

    update_last_active(context)  # Обновляем время последней активности

# Функция для повышения уровня
async def level_up(update: Update, context: CallbackContext):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("🚀 Хочешь повысить свой уровень? Введи кодовое слово ниже:")
    context.user_data['step'] = 'get_level_code'
    update_last_active(context)  # Обновляем время последней активности

# Функция для начала тестирования
async def start_testing(query, context):
    user = query.from_user
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT level FROM users WHERE username = ?', (user.username,))
    user_data = cursor.fetchone()
    conn.close()

    if not user_data:
        await query.edit_message_text("❌ Пользователь не найден.")
        return

    user_level = user_data['level']

    # Проверка финальных уровней
    if user_level in ["🥳Защитил финальное задание!🥳", "🎉Прошёл тестовое собеседование!🎉", "🦸‍♂️Коллега!🦸‍♂️"]:
        if user_level == "🥳Защитил финальное задание!🥳":
            await query.edit_message_text("Ты молодец! Ты уже прошёл все тесты, защитил финальное задание и тебе осталось только тестовое собеседование!")
        elif user_level == "🎉Прошёл тестовое собеседование!🎉":
            await query.edit_message_text("Ты молодец! Ты уже прошёл все тесты, защитил финальное задание, а также успешно прошёл тестовое собеседование! Тебе осталось только устроиться на работу!")
        elif user_level == "🦸‍♂️Коллега!🦸‍♂️":
            await query.edit_message_text("Ты молодец! Ты уже прошёл все тесты и стал нереально крутым системным аналитиком!")
        
        keyboard = [[InlineKeyboardButton("Назад", callback_data='menu')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(reply_markup=reply_markup)
        return

    if user_level == '❌ Ты пока не обучаешься на курсе! Приступай скорее! ❌':
        await query.edit_message_text("❌ Тестирование недоступно. Повысь свой уровень! 🚀")
        return

    # Определяем блок тестирования на основе уровня пользователя
    block = get_block_from_level(user_level)
    if not block:
        await query.edit_message_text("❌ Тестирование недоступно для твоего уровня. Продолжай учиться! 💪")
        return

    # Выбираем случайные вопросы
    questions = get_random_questions(block)
    if not questions:
        # Если вопросы не найдены, показываем сообщение и кнопку "Назад"
        keyboard = [[InlineKeyboardButton("Назад", callback_data='menu')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("❌ Вопросы для тестирования не найдены. Попробуй позже. 😊", reply_markup=reply_markup)
        return

    # Сохраняем вопросы и начальные данные в контексте
    context.user_data['questions'] = questions
    context.user_data['current_question'] = 0
    context.user_data['answers'] = []

    # Показываем первый вопрос
    await show_question(query, context)

# Получение блока тестирования на основе уровня пользователя
def get_block_from_level(level):
    level_to_block = {
        "❌ Ты пока не обучаешься на курсе! Приступай скорее! ❌": None,
        "🤝Добро пожаловать на курс!🤝": 1,
        "👍Защитил первый блок!👍": 2,
        "✊Защитил второй блок!✊": 3,
        "💪Защитил третий блок!💪": 4,
        "🔥Защитил четвёртый блок!🔥": 5,
        "🎉Защитил пятый блок!🎉": 6,
        "🥳Защитил финальное задание!🥳": None,
        "🎉Прошёл тестовое собеседование!🎉": None,
        "🦸‍♂️Коллега!🦸‍♂️": None,
    }
    return level_to_block.get(level)
    
# Получение случайных вопросов для блока
def get_random_questions(block, limit=10):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM questions WHERE block = ? ORDER BY RANDOM() LIMIT ?', (block, limit))
    questions = cursor.fetchall()
    conn.close()
    return questions
    
# Показ вопроса
async def show_question(query, context):
    questions = context.user_data['questions']
    current_question = context.user_data['current_question']
    question = questions[current_question]

    # Формируем текст вопроса и варианты ответа
    question_text = f"Вопрос {current_question + 1}:\n{question['question_text']}\n\n"
    options = [question['option1'], question['option2'], question['option3'], question['option4']]
    for i, option in enumerate(options, start=1):
        question_text += f"{i}. {option}\n"

    # Создаем инлайн-кнопки для ответов
    keyboard = [
        [InlineKeyboardButton(str(i), callback_data=f"answer_{i}") for i in range(1, 5)],  # Кнопки с вариантами ответов
        [InlineKeyboardButton("Назад в меню", callback_data="menu")]  # Кнопка для возврата в меню
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Отправляем вопрос пользователю
    await query.edit_message_text(question_text, reply_markup=reply_markup)
    
# Обработчик ответов
async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Создаем локальное соединение с базой данных
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = update.callback_query
        await query.answer()

        # Получаем выбранный ответ
        answer = int(query.data.split("_")[1])
        context.user_data['answers'].append(answer)

        # Переходим к следующему вопросу
        context.user_data['current_question'] += 1
        if context.user_data['current_question'] < len(context.user_data['questions']):
            await show_question(query, context)
        else:
            # Завершаем тест и подсчитываем результаты
            await finish_test(query, context)

    except Exception as e:
        logger.error(f"Ошибка в handle_answer: {e}", exc_info=True)

    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()
        
# Завершение теста
async def finish_test(query, context):
    # Создаем локальное соединение с базой данных
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        questions = context.user_data['questions']
        answers = context.user_data['answers']
        correct_answers = 0
        wrong_questions = []

        for i, question in enumerate(questions):
            if answers[i] == question['correct_option']:
                correct_answers += 1
            else:
                wrong_questions.append(question['question_text'])

        total_questions = len(questions)
        result = "🎉 Ты сдал(а)! Поздравляю! 🎉" if (correct_answers / total_questions) > 0.81 else "❌ Увы, но ты не сдал(а). Попробуй ещё раз! 💪"

        # Формируем статистику
        statistics = f"🎉 Правильные ответы: {correct_answers} из {total_questions}\n"
        statistics += f"❌ Неправильные ответы: {total_questions - correct_answers}\n"
        if wrong_questions:
            statistics += "📝 Вопросы с ошибками:\n"
            for i, question in enumerate(wrong_questions, start=1):
                statistics += f"{i}. {question}\n"

        # Отправляем результат пользователю
        keyboard = [[InlineKeyboardButton("Назад в меню", callback_data="menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"🎉 Тест завершён! Ты умничка! 🎉\n{result}\n{statistics}", reply_markup=reply_markup)

        # Уведомляем администраторов
        await notify_admins(query.from_user, result, statistics, context)

        # Очищаем данные теста
        context.user_data.clear()

    except Exception as e:
        logger.error(f"Ошибка в finish_test: {e}", exc_info=True)

    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()
    
# Уведомление администраторов
async def notify_admins(user, result, statistics, context):    
    # Создаем локальное соединение с базой данных
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        message = f"✅ {user.full_name} (@{user.username}) прошёл тест!\n"
        message += f"Его результат: {result}\n"
        message += f"Его статистика:\n{statistics}"

        cursor.execute('SELECT username, chat_id FROM users WHERE is_admin = TRUE')
        admins = cursor.fetchall()
        conn.close()

        if not admins:
            logger.error("В базе данных нет администраторов.")
            return

        for admin in admins:
            chat_id = admin['chat_id']
            username = admin['username']
            if not chat_id:
                logger.error(f"Администратор {username} не имеет chat_id.")
                continue

            logger.info(f"Попытка отправить сообщение администратору {username} (chat_id: {chat_id}).")
            try:
                await context.bot.send_message(chat_id=chat_id, text=message)
                logger.info(f"Уведомление отправлено администратору {username} (chat_id: {chat_id}).")
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения администратору {username} (chat_id: {chat_id}): {e}")
                
            except Exception as e:
                logger.error(f"Не удалось отправить сообщение администратору {username} (chat_id: {chat_id}): {e}")
    except Exception as e:
        logger.error(f"Ошибка в notify_admins: {e}", exc_info=True)

    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()
            
# Добавление вопроса
async def add_question(query, context):
    message = (
        "📝 Введите данные для добавления вопроса в формате:\n\n"
        "Вопрос | Вариант1 | Вариант2 | Вариант3 | Вариант4 | Правильный вариант (1-4) | Блок\n\n"
        "Пример 1:\n"
        "Что такое Python? | Язык программирования | База данных | Операционная система | Фреймворк | 1 | 1\n\n"
        "Пример 2:\n"
        "Какой тип данных используется для хранения целых чисел? | int | str | float | bool | 1 | 2"
    )
    await query.edit_message_text(message)
    context.user_data['awaiting_add_question'] = True
    
# Обработчик добавления вопроса
async def handle_add_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    parts = text.split('|')
    if len(parts) != 7:
        await update.message.reply_text("❌ Ой, что-то пошло не так! Неверный формат. Попробуй ещё раз. 😊")
        return

    question_text = parts[0].strip()
    option1 = parts[1].strip()
    option2 = parts[2].strip()
    option3 = parts[3].strip()
    option4 = parts[4].strip()
    correct_option = int(parts[5].strip())
    block = int(parts[6].strip())

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Проверяем, существует ли таблица questions
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='questions'")
        if not cursor.fetchone():
            await update.message.reply_text("Ошибка: Таблица questions не существует.")
            return

        # Проверяем, существуют ли все столбцы
        cursor.execute("PRAGMA table_info(questions)")
        columns = [column[1] for column in cursor.fetchall()]
        required_columns = ["block", "question_text", "option1", "option2", "option3", "option4", "correct_option"]
        for column in required_columns:
            if column not in columns:
                await update.message.reply_text(f"Ошибка: Столбец {column} отсутствует в таблице questions.")
                return

        # Проверяем, существует ли уже такой вопрос
        cursor.execute('SELECT * FROM questions WHERE question_text = ?', (question_text,))
        if cursor.fetchone():
            await update.message.reply_text("Ошибка: Такой вопрос уже существует.")
            return

        # Добавляем вопрос
        cursor.execute('''
            INSERT INTO questions (block, question_text, option1, option2, option3, option4, correct_option)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (block, question_text, option1, option2, option3, option4, correct_option))
        conn.commit()

        logger.info(f"Данные вопроса: {parts}")

        await update.message.reply_text("✅ Вопрос успешно добавлен!")
    except Exception as e:
        logger.error(f"Ошибка в handle_add_question: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при добавлении вопроса. Попробуй ещё раз. 😊")
    finally:
        if conn:
            conn.close()

    # Сбрасываем флаг
    context.user_data.pop('awaiting_add_question', None)

    # Возвращаем пользователя в меню
    await start(update, context)
    
# Редактирование вопроса
async def edit_question(query, context):
    await query.edit_message_text("Введите текст вопроса и новые данные в формате:\n"
                                 "Текст вопроса | Новый вопрос | Вариант1 | Вариант2 | Вариант3 | Вариант4 | Правильный вариант (1-4) | Блок")
    context.user_data['awaiting_edit_question'] = True
    
async def handle_edit_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    parts = text.split('|')
    if len(parts) != 8:
        await update.message.reply_text("❌ Ой, что-то пошло не так! Неверный формат. Попробуй ещё раз. 😊")
        return

    old_question_text = parts[0].strip()
    new_question_text = parts[1].strip()
    option1 = parts[2].strip()
    option2 = parts[3].strip()
    option3 = parts[4].strip()
    option4 = parts[5].strip()
    correct_option = int(parts[6].strip())
    block = int(parts[7].strip())

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Проверяем, существует ли вопрос
        cursor.execute('SELECT * FROM questions WHERE question_text = ?', (old_question_text,))
        if not cursor.fetchone():
            await update.message.reply_text("❌ Вопрос не найден.")
            return

        # Обновляем вопрос
        cursor.execute('''
            UPDATE questions
            SET question_text = ?, option1 = ?, option2 = ?, option3 = ?, option4 = ?, correct_option = ?, block = ?
            WHERE question_text = ?
        ''', (new_question_text, option1, option2, option3, option4, correct_option, block, old_question_text))
        conn.commit()

        await update.message.reply_text("✅ Вопрос успешно обновлён!")
    except Exception as e:
        logger.error(f"Ошибка в handle_edit_question: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при обновлении вопроса. Попробуй ещё раз. 😊")
    finally:
        if conn:
            conn.close()

    # Сбрасываем флаг
    context.user_data.pop('awaiting_edit_question', None)

    # Возвращаем пользователя в меню
    await start(update, context)
    
# Удаление вопроса
async def delete_question(query, context):
    await query.edit_message_text("Введите текст вопроса для удаления:")
    context.user_data['awaiting_delete_question'] = True
    
async def handle_delete_question(update: Update, context: ContextTypes.DEFAULT_TYPE):
    question_text = update.message.text.strip()

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Проверяем, существует ли вопрос
        cursor.execute('SELECT * FROM questions WHERE question_text = ?', (question_text,))
        if not cursor.fetchone():
            await update.message.reply_text("❌ Вопрос не найден.")
            return

        # Удаляем вопрос
        cursor.execute('DELETE FROM questions WHERE question_text = ?', (question_text,))
        conn.commit()

        await update.message.reply_text("✅ Вопрос успешно удалён!")
    except Exception as e:
        logger.error(f"Ошибка в handle_delete_question: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при удалении вопроса. Попробуй ещё раз. 😊")
    finally:
        if conn:
            conn.close()

    # Сбрасываем флаг
    context.user_data.pop('awaiting_delete_question', None)

    # Возвращаем пользователя в меню
    await start(update, context)
    
# Просмотр вопросов с пагинацией
async def view_all_questions(query, context, page=0):
    conn = get_db_connection()
    cursor = conn.cursor()
    # Получаем вопросы с пагинацией
    cursor.execute('SELECT * FROM questions ORDER BY block, id LIMIT 15 OFFSET ?', (page * 15,))
    questions = cursor.fetchall()
    conn.close()

    if not questions:
        # Если вопросы не найдены, показываем сообщение и кнопку "Назад"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='menu')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("❌ Вопросы не найдены.", reply_markup=reply_markup)
        return

    # Формируем текст с вопросами
    questions_text = "📚 Все вопросы:\n\n"
    current_block = None

    for question in questions:
        if question['block'] != current_block:
            # Добавляем пустую строку между блоками, если это не первый блок
            if current_block is not None:
                questions_text += "\n"
            questions_text += f"📖 Блок {question['block']}:\n"
            current_block = question['block']
        questions_text += f"{question['id']}. {question['question_text']}\n"

    # Создаем кнопки для пагинации
    keyboard = []
    if page > 0:
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"view_all_questions_{page - 1}")])
    if len(questions) == 15:
        keyboard.append([InlineKeyboardButton("Вперед ➡️", callback_data=f"view_all_questions_{page + 1}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад в меню", callback_data="menu")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Отправляем вопросы пользователю
    await query.edit_message_text(questions_text, reply_markup=reply_markup)
    
# Просмотр уровней пользователей
async def view_user_levels(query, context):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT username, level FROM users')
    users = cursor.fetchall()
    conn.close()

    if not users:
        await query.edit_message_text("Пользователи не найдены.")
        return

    # Формируем текст с уровнями пользователей
    levels_text = "Уровни пользователей:\n\n"
    for user in users:
        levels_text += f"@{user['username']}: {user['level']}\n"

    # Добавляем кнопку "Назад в меню"
    keyboard = [[InlineKeyboardButton("Назад в меню", callback_data="menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(levels_text, reply_markup=reply_markup)
    
# Изменение уровня пользователя
async def change_user_level(query, context):
    await query.edit_message_text("Введите username пользователя и новый уровень через пробел (например, user123 🤝Добро пожаловать на курс!🤝):")
    context.user_data['awaiting_change_user_level'] = True
    
# Обработчик изменения уровня пользователя
async def handle_change_user_level(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    parts = text.split()
    if len(parts) < 2:
        await update.message.reply_text("❌ Ой, что-то пошло не так! Неверный формат. Попробуй ещё раз. 😊")
        return

    username = parts[0]
    new_level = ' '.join(parts[1:])

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Проверяем, существует ли пользователь
        cursor.execute('SELECT id FROM users WHERE username = ?', (username,))
        user_data = cursor.fetchone()
        if not user_data:
            await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
            return

        # Обновляем уровень пользователя
        cursor.execute('UPDATE users SET level = ? WHERE username = ?', (new_level, username))
        conn.commit()

        await update.message.reply_text(f"✅ Уровень пользователя @{username} успешно изменён на: {new_level}")
    except Exception as e:
        logger.error(f"Ошибка в handle_change_user_level: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при изменении уровня. Попробуй ещё раз. 😊")
    finally:
        if conn:
            conn.close()

    # Сбрасываем флаг
    context.user_data.pop('awaiting_change_user_level', None)

    # Возвращаем пользователя в меню
    await start(update, context)
    
# Массовое добавление вопросов
async def bulk_add_questions(query, context):
    await query.edit_message_text("Введите вопросы в формате:\n\n"
                                 "Вопрос1 | Вариант1 | Вариант2 | Вариант3 | Вариант4 | Правильный вариант (1-4) | Блок\n"
                                 "Вопрос2 | Вариант1 | Вариант2 | Вариант3 | Вариант4 | Правильный вариант (1-4) | Блок\n\n"
                                 "Пример:\n"
                                 "Что такое Python? | Язык программирования | База данных | Операционная система | Фреймворк | 1 | 1\n"
                                 "Какой тип данных используется для хранения целых чисел? | int | str | float | bool | 1 | 2")
    # Устанавливаем флаг
    context.user_data['awaiting_bulk_add_questions'] = True
    
# Обработчик массового добавления вопросов
async def handle_bulk_add_questions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    questions = text.split('\n')

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        for question in questions:
            parts = question.split('|')
            if len(parts) != 7:
                await update.message.reply_text(f"Неверный формат вопроса: {question}")
                continue

            question_text = parts[0].strip()
            option1 = parts[1].strip()
            option2 = parts[2].strip()
            option3 = parts[3].strip()
            option4 = parts[4].strip()
            correct_option = int(parts[5].strip())
            block = int(parts[6].strip())

            # Проверяем, существует ли уже такой вопрос
            cursor.execute('SELECT * FROM questions WHERE question_text = ?', (question_text,))
            if cursor.fetchone():
                await update.message.reply_text(f"Ошибка: Вопрос '{question_text}' уже существует.")
                continue

            # Добавляем вопрос
            cursor.execute('''
                INSERT INTO questions (block, question_text, option1, option2, option3, option4, correct_option)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (block, question_text, option1, option2, option3, option4, correct_option))

            logger.info(f"Данные вопросов: {questions}")

        conn.commit()
        await update.message.reply_text("✅ Вопросы успешно добавлены!")
    except Exception as e:
        logger.error(f"Ошибка в handle_bulk_add_questions: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при добавлении вопросов.")
    finally:
        if conn:
            conn.close()
            
    # Сбрасываем флаг
    context.user_data.pop('awaiting_bulk_add_questions', None)
    
    # Возвращаем пользователя в меню
    await start(update, context)

async def manage_testing(query, context):
    keyboard = [
        [InlineKeyboardButton("➕ Добавить вопрос", callback_data='add_question')],
        [InlineKeyboardButton("✏️ Редактировать вопрос", callback_data='edit_question')],
        [InlineKeyboardButton("🗑 Удалить вопрос", callback_data='delete_question')],
        [InlineKeyboardButton("👀 Просмотреть вопросы", callback_data='view_all_questions_0')],
        [InlineKeyboardButton("📊 Просмотреть уровни пользователей", callback_data='view_user_levels')],
        [InlineKeyboardButton("🔄 Изменить уровень пользователя", callback_data='change_user_level')],
        [InlineKeyboardButton("📚 Массовое добавление вопросов", callback_data='bulk_add_questions')],
        [InlineKeyboardButton("🔙 Назад", callback_data='admin')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("Управление тестированием:", reply_markup=reply_markup)

# Обработка ввода ФИО
async def handle_full_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    # Создаем локальное соединение с базой данных
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        full_name = update.message.text
        user = update.effective_user

        # Логируем ввод данных
        logger.info(f"User {user.username} provided full_name: {full_name}")

        # Проверяем, является ли пользователь администратором
        is_admin_user = is_admin(user.username)

        # Логируем данные перед сохранением
        logger.info(f"Регистрируем пользователя: username={user.username}, is_admin={is_admin_user}")

        # Сохраняем пользователя в базу данных
        cursor.execute(
            "INSERT OR REPLACE INTO users (id, username, full_name, is_admin, joined_at) VALUES (?, ?, ?, ?, ?)",
            (user.id, user.username, full_name, is_admin_user, datetime.now().strftime('%Y-%m-%d'))
        )
        conn.commit()

        await update.message.reply_text(f"🎉 Отлично, {full_name}! Теперь ты зарегистрирован(а)! Давай начнём! 🚀")
        await start(update, context)
    except Exception as e:
        logger.error(f"Ошибка в handle_full_name: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

async def set_user_meeting_reminder_time(update: Update, context: CallbackContext):
    logger.info("Кнопка 'Напоминания пользователей' нажата")
    await update.callback_query.edit_message_text("⏰ Хочешь настроить напоминания? \nВведи время за которое присылать уведомление о назначенных встречах в формате: ММ. \nНапример: 30 (за 30 минут до встречи).")
    context.user_data['step'] = 'get_user_meeting_reminder_time'
    update_last_active(context)

async def handle_user_meeting_reminder_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Обработка времени напоминания для пользователя")
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            minutes = int(update.message.text)
            if minutes < 0:
                await update.message.reply_text("❌ Время не может быть отрицательным. Попробуй ещё раз. Например: 30.")
                return

            # Сохраняем время напоминания в базу данных
            cursor.execute(
                "INSERT OR REPLACE INTO reminders (user_id, reminder_type, user_meeting_reminder_minutes) VALUES (?, ?, ?)",
                (update.effective_user.id, 'user_meeting_reminder', minutes)
            )
            conn.commit()

            logger.info(f"Время напоминания сохранено: {minutes} минут для пользователя {update.effective_user.username}")

            await update.message.reply_text(f"🔔 Хорошо! Теперь я буду присылать тебе уведомления за {minutes} минут до встречи! Не пропусти! 🕒")
            await start(update, context)
            context.user_data.clear()
        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Введи число минут.")

    except Exception as e:
        logger.error(f"Ошибка в handle_user_meeting_reminder_time: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

async def set_meeting_reminder_time(update: Update, context: CallbackContext):
    logger.info("Функция set_meeting_reminder_time вызвана")
    if not is_admin(update.effective_user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    await update.callback_query.edit_message_text("За какое время присылать уведомление о назначенных встречах? Введи в формате ММ:")
    context.user_data['step'] = 'get_meeting_reminder_time'
    logger.info(f"Установлен шаг: {context.user_data['step']}")  # Логируем установленный шаг
    update_last_active(context)

async def handle_meeting_reminder_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Функция handle_meeting_reminder_time вызвана")
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            minutes = int(update.message.text)
            if minutes < 0:
                await update.message.reply_text("❌ Время не может быть отрицательным. Попробуйте снова.")
                return

            # Сохраняем время напоминания в базу данных
            cursor.execute(
                "INSERT OR REPLACE INTO reminders (user_id, reminder_type, meeting_reminder_minutes) VALUES (?, ?, ?)",
                (update.effective_user.id, 'meeting_reminder', minutes)
            )
            conn.commit()

            await update.message.reply_text(f"Хорошо, теперь буду присылать тебе уведомления за {minutes} минут до назначенных встреч!")
            await start(update, context)
            context.user_data.clear()
        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Введи число минут.")

    except Exception as e:
        logger.error(f"Ошибка в handle_meeting_reminder_time: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

# Обработка кнопок
async def button(update: Update, context: CallbackContext):
    query = update.callback_query
    await query.answer()
    if query.data == 'stop':
        await start(update, context)
        return

    if query.data == 'back':
        if 'menu_stack' in context.user_data and len(context.user_data['menu_stack']) > 1:
            # Удаляем текущее меню из стека
            context.user_data['menu_stack'].pop()
            
            # Извлекаем предыдущее меню из стека
            previous_menu = context.user_data['menu_stack'][-1]
            
            # Вызываем предыдущее меню
            await previous_menu(update, context)
        else:
            # Если стек пуст или содержит только одно меню, возвращаемся в главное меню
            await start(update, context)
        return

    # Обработка кнопок пагинации для расписания
    if query.data.startswith('schedule_page_'):
        page = int(query.data.split('_')[-1])  # Извлекаем номер страницы
        context.user_data['schedule_page'] = page  # Обновляем текущую страницу
        await view_schedule(update, context)  # Вызываем функцию для отображения новой страницы
        return

    # Обработка кнопок пагинации для прошедших встреч
    if query.data.startswith('past_events_page_'):
        page = int(query.data.split('_')[-1])  # Извлекаем номер страницы
        context.user_data['past_events_page'] = page  # Обновляем текущую страницу
        await past_events(update, context)  # Вызываем функцию для отображения новой страницы
        return

    # Логируем нажатие кнопки
    logger.info(f"Нажата кнопка: {query.data}")

    # Остальные обработчики кнопок
    if query.data == 'schedule':
        await schedule_menu(update, context)
    elif query.data == 'manage_users':
        await manage_users(update, context)
    elif query.data == 'list_users':
        await list_users(update, context)
    elif query.data == 'manage_statuses':
        await manage_statuses(update, context)
    elif query.data == 'delete_user':
        await delete_user(update, context)
    elif query.data == 'block_user':
        await block_user(update, context)
    elif query.data == 'progress':
        await progress_menu(update, context)
    elif query.data == 'help':
        await request_help(update, context)
    elif query.data == 'admin':
        await admin_menu(update, context)
    elif query.data == 'add_event':
        await add_event(update, context)
    elif query.data == 'view_schedule':
        await view_schedule(update, context)
    elif query.data == 'my_events':
        await my_events(update, context)
    elif query.data == 'past_events':
        await past_events(update, context)
    elif query.data == 'delete_event':
        await delete_event(update, context)
    elif query.data == 'reschedule_event':
        await reschedule_event(update, context)
    elif query.data == 'view_progress':
        await view_progress(update, context)
    elif query.data == 'level_up':
        await level_up(update, context)
    elif query.data == 'view_all_progress':
        await view_all_progress(update, context)
    elif query.data == 'notification_settings':
        await notification_settings(update, context)
    elif query.data == 'broadcast':
        await broadcast(update, context)
    elif query.data == 'manage_meeting_reminders':
        await manage_meeting_reminders(update, context)
    elif query.data == 'set_one_time_reminder':
        await set_one_time_reminder(update, context)
    elif query.data == 'set_recurring_reminder':
        await set_recurring_reminder(update, context)
    elif query.data == 'view_notifications':
        await view_notifications(update, context)
    elif query.data == 'delete_notification':
        await delete_notification(update, context)
    elif query.data == 'view_statistics':
        await view_statistics(update, context)
    elif query.data == 'show_level_codes':
        await show_level_codes(update, context)
    elif query.data == 'block_day':
        await block_day(update, context)
    elif query.data == 'enable_reminders':
        await enable_reminders(update, context)
    elif query.data == 'change_reminders':
        await change_reminders(update, context)
    elif query.data == 'disable_reminders':
        await disable_reminders(update, context)
    elif query.data == 'meeting_reminders':
        await set_meeting_reminder_time(update, context)
    elif query.data == 'user_reminders':
        await set_user_meeting_reminder_time(update, context)
    elif query.data == 'testing':
        await start_testing(query, context)
    elif query.data == 'manage_testing':
        await manage_testing(query, context)
    elif query.data == 'add_question':
        await add_question(query, context)
    elif query.data == 'edit_question':
        await edit_question(query, context)
    elif query.data == 'delete_question':
        await delete_question(query, context)
    elif query.data.startswith('answer_'):
        await handle_answer(update, context)
    elif query.data == 'cancel_test':
        context.user_data.clear()
        await query.edit_message_text("🚫 Тестирование отменено. Если передумаешь, всегда можно начать заново. 😉")
        await menu(update, context)
    elif query.data.startswith('view_all_questions_'):
        page = int(query.data.split('_')[3])
        await view_all_questions(query, context, page)
    elif query.data == 'view_user_levels':
        await view_user_levels(query, context)
    elif query.data == 'change_user_level':
        await change_user_level(query, context)
    elif query.data == 'bulk_add_questions':
        await bulk_add_questions(query, context)
    elif query.data == 'menu':
        await start(update, context)

    # Если кнопка не распознана, отправляем основное меню
    else:
        logger.warning(f"Неизвестная кнопка: {query.data}")
        await start(update, context)
    
# Меню админа
async def admin_menu(update: Update, context: CallbackContext):
    # Добавляем текущее меню в стек, если его там еще нет
    if 'menu_stack' not in context.user_data:
        context.user_data['menu_stack'] = []
    
    # Проверяем, что текущее меню еще не в стеке
    if not context.user_data['menu_stack'] or context.user_data['menu_stack'][-1] != admin_menu:
        context.user_data['menu_stack'].append(admin_menu)
        logger.info(f"Добавлено меню 'admin_menu' в стек. Текущий стек: {[func.__name__ for func in context.user_data['menu_stack']]}")

    # Получаем объект пользователя
    user = update.effective_user

    # Проверяем, является ли пользователь администратором
    if not is_admin(user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    keyboard = [
        [InlineKeyboardButton("🔔 Настройка уведомлений", callback_data='notification_settings'), InlineKeyboardButton("👤 Пользователи", callback_data='manage_users')],
        [InlineKeyboardButton("📩 Рассылка", callback_data='broadcast'), InlineKeyboardButton("📊 Статистика", callback_data='view_statistics')],
        [InlineKeyboardButton("🔑 Кодовые слова", callback_data='show_level_codes'), InlineKeyboardButton("🚫 Заблокировать день", callback_data='block_day')],
        [InlineKeyboardButton("🎓 Управление тестированием", callback_data='manage_testing')],  # Новая кнопка
        [InlineKeyboardButton("🔙 Назад", callback_data='back')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query = update.callback_query

    # Проверяем, изменились ли текст или разметка
    current_text = "👑 Меню админа:"
    if query.message.text != current_text or query.message.reply_markup != reply_markup:
        await query.edit_message_text(text=current_text, reply_markup=reply_markup)
    else:
        logger.info("🔍 Сообщение не изменилось. Пропускаем обновление. 😉")

    update_last_active(context)  # Обновляем время последней активности
    
async def show_level_codes(update: Update, context: CallbackContext):
    # Формируем текст с кодовыми словами и уровнями
    level_codes_text = "🔑 Кодовые слова для повышения уровня:\n\n"
    for code, level in LEVEL_CODES.items():
        level_codes_text += f"🔹 {code} → {level}\n"

    # Добавляем кнопку "Назад"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Редактируем текущее сообщение
    await update.callback_query.edit_message_text(level_codes_text, reply_markup=reply_markup)

# Запрос помощи
async def request_help(update: Update, context: CallbackContext):
    await update.callback_query.edit_message_text("🆘 Нужна помощь? Расскажи, что случилось, и я позову менторов! \nНапример: Не могу разобраться с задачей по SQL.")
    context.user_data['step'] = 'get_help_message'  # Устанавливаем шаг для обработки следующего сообщения
    update_last_active(context)

async def handle_help_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    help_message = update.message.text
    user = update.effective_user

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        # Отправляем уведомление администраторам
        for admin in ADMINS:
            try:
                cursor.execute("SELECT id FROM users WHERE username = ?", (admin,))
                admin_id = cursor.fetchone()
                if admin_id:
                    await context.bot.send_message(
                        chat_id=admin_id[0],
                        text=f"🆘 Пользователь @{user.username} запросил помощь!\n📝 Тема: {help_message}"
                    )
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения администратору {admin}: {e}")

        # Отправляем подтверждение пользователю
        await update.message.reply_text("👌 Хорошо! Я уже зову менторов. Они скоро свяжутся с тобой! 🤝")

        # Очищаем состояние (step) после успешной обработки
        if 'step' in context.user_data:
            del context.user_data['step']

        # Возвращаем пользователя в главное меню
        await start(update, context)

    except Exception as e:
        logger.error(f"Ошибка в handle_help_message: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при отправке запроса на помощь. Попробуй ещё раз.")
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

# Функция для добавления события
async def add_event(update: Update, context: CallbackContext):
    user = update.effective_user

    if is_admin(user.username):
        # Для администратора
        await update.callback_query.edit_message_text("👤 Хочешь назначить встречу для другого пользователя? Введи его имя в формате: Фамилия Имя. \nНапример: Иванов Иван.")
        context.user_data['step'] = 'get_user_full_name'
    else:
        # Для обычного пользователя
        await update.callback_query.edit_message_text("Хорошо! 📆 Хочешь назначить встречу? Введи дату в формате: ДД.ММ.ГГГГ. \nНапример: 15.03.2025")
        context.user_data['step'] = 'get_event_date'
    update_last_active(context)  # Обновляем время последней активности

# Обработка ввода ФИО пользователя для администратора
async def handle_user_full_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    full_name = update.message.text
    context.user_data['user_full_name'] = full_name
    await update.message.reply_text("👤Введите Ник пользователя, которому ты назначаешь встречу, в формате: @Ник_пользователя: \nНапример: @ivanov.")
    context.user_data['step'] = 'get_user_username'
    update_last_active(context)  # Обновляем время последней активности

async def handle_user_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    username = update.message.text
    if not username.startswith('@'):
        await update.message.reply_text("❌ Ник должен начинаться с символа @. Попробуйте снова.")
        return

    context.user_data['user_username'] = username
    await update.message.reply_text("📆 Хочешь назначить встречу? Введи дату в формате: ДД.ММ.ГГГГ. \nНапример: 01.09.2025")
    context.user_data['step'] = 'get_event_date'
    update_last_active(context)  # Обновляем время последней активности
    
# Обработка ввода даты
async def handle_event_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    event_date = update.message.text
    try:
        # Проверка формата даты
        event_date_obj = datetime.strptime(event_date, '%d.%m.%Y')
        event_date_obj = MOSCOW_TZ.localize(event_date_obj)  # Устанавливаем московское время
        context.user_data['event_date'] = event_date_obj.strftime('%d.%m.%Y')
        await update.message.reply_text("⏰ Отлично! Теперь введи время встречи в формате: ЧЧ:ММ. \nНапример: 19:00.")
        context.user_data['step'] = 'get_event_time'
        update_last_active(context)  # Обновляем время последней активности
    except ValueError:
        await update.message.reply_text("❌ Неверный формат даты. Введи в формате ДД.ММ.ГГГГ.")

async def handle_event_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        event_time = update.message.text
        logger.info(f"Введенное время: {event_time}")  # Логируем введенное время

        try:
            # Проверка формата времени
            datetime.strptime(event_time, '%H:%M')  # Проверяем, что время в формате ЧЧ:ММ
            hour, minute = map(int, event_time.split(':'))  # Разделяем на часы и минуты
            logger.info(f"Время успешно разобрано: час={hour}, минута={minute}")  # Логируем разобранное время

            user = update.effective_user
            if not is_admin(user.username):
                # Для обычного пользователя проверяем, что время встречи в пределах 19:00-23:00
                if hour < 19 or hour > 23:
                    await update.message.reply_text("❌ Встречи можно назначать только с 19:00 до 23:00.")
                    return

                # Округление времени
                if minute < 30:
                    event_time = f"{hour}:00"
                else:
                    event_time = f"{hour + 1}:00" if hour < 23 else "23:00"
                logger.info(f"Округленное время: {event_time}")  # Логируем округленное время

            # Получаем текущую дату и время
            current_time = get_moscow_time()
            event_date = context.user_data['event_date']
            logger.info(f"Текущая дата из контекста: {event_date}")  # Логируем текущую дату

            # Преобразуем дату из формата ДД.ММ.ГГГГ в YYYY-MM-DD
            try:
                event_date_obj = datetime.strptime(event_date, '%d.%m.%Y')
                event_date_formatted = event_date_obj.strftime('%Y-%m-%d')  # Преобразуем в YYYY-MM-DD
                logger.info(f"Преобразованная дата: {event_date_formatted}")  # Логируем преобразованную дату
            except ValueError:
                await update.message.reply_text("❌ Неверный формат даты. Введи в формате ДД.ММ.ГГГГ.")
                return

            # Преобразуем введённые дату и время в объект datetime
            event_datetime = datetime.strptime(f"{event_date_formatted} {event_time}", '%Y-%m-%d %H:%M')
            event_datetime = MOSCOW_TZ.localize(event_datetime)
            logger.info(f"Преобразованное время: {event_datetime}")  # Логируем преобразованное время

            # Проверяем, что встреча назначена в будущем
            if event_datetime <= current_time:
                await update.message.reply_text("❌ Встреча должна быть назначена на будущее время.")
                return

            # Дополнительная проверка: если встреча назначена на текущий день, но на прошедшее время
            if event_datetime.date() == current_time.date() and event_datetime.time() <= current_time.time():
                await update.message.reply_text("❌ Встреча должна быть назначена на будущее время.")
                return

            # Сохраняем время в московском времени
            context.user_data['event_time'] = event_datetime.strftime('%H:%M')
            context.user_data['event_date'] = event_datetime.strftime('%Y-%m-%d')
            logger.info(f"Сохраненное время: {context.user_data['event_time']}")  # Логируем сохраненное время

            # Проверка на занятость слота
            cursor.execute("SELECT * FROM schedule WHERE event_date = ? AND event_time = ?", 
                           (context.user_data['event_date'], context.user_data['event_time']))
            if cursor.fetchone():
                if is_admin(user.username):
                    await update.message.reply_text("⚠️ На это время уже стоит встреча, но вы можете продолжить.")
                else:
                    await update.message.reply_text("❌ На это время уже есть встреча. Выбери другое время. Например: 20:00.")
                    return

            await update.message.reply_text("📝 Введи тему встречи. \nНапример: Обсуждение проекта.")
            context.user_data['step'] = 'get_event_topic'
            update_last_active(context)
        except ValueError as e:
            logger.error(f"Ошибка при обработке времени: {e}")  # Логируем ошибку
            await update.message.reply_text("❌ Неверный формат времени. Введи в формате ЧЧ:ММ.")
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

# Обработка ввода темы встречи
async def handle_event_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    event_topic = update.message.text
    user = update.effective_user

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if is_admin(user.username):
            # Для администратора
            full_name = context.user_data['user_full_name']
            username = context.user_data['user_username']

            # Находим ID пользователя по нику
            cursor.execute("SELECT id FROM users WHERE username = ?", (username[1:],))  # Убираем @
            user_id = cursor.fetchone()
            if not user_id:
                await update.message.reply_text("❌ Пользователь с таким ником не найден.")
                return
            user_id = user_id[0]
        else:
            # Для обычного пользователя
            user_id = user.id
            full_name = user.full_name
            username = user.username

        # Проверяем, существуют ли ключи в context.user_data
        if 'event_date' not in context.user_data or 'event_time' not in context.user_data:
            await update.message.reply_text("❌ Произошла ошибка. Попробуйте снова.")
            return

        event_date = context.user_data['event_date']
        event_time = context.user_data['event_time']

        # Сохраняем встречу в базу данных
        cursor.execute(
            "INSERT INTO schedule (user_id, event_name, event_date, event_time) VALUES (?, ?, ?, ?)",
            (user_id, event_topic, event_date, event_time)
        )
        conn.commit()

        # Отправляем уведомление администраторам
        for admin in ADMINS:
            try:
                cursor.execute("SELECT id FROM users WHERE username = ?", (admin,))
                admin_id = cursor.fetchone()
                if admin_id:
                    await context.bot.send_message(
                        chat_id=admin_id[0],
                        text=f"Пользователь @{username} назначил встречу {event_date} на {event_time}!"
                    )
            except Exception as e:
                print(f"Ошибка при отправке сообщения администратору {admin}: {e}")

        if is_admin(user.username):
            # Отправляем уведомление пользователю
            await context.bot.send_message(
                chat_id=user_id,
                text=f"Администратор @{user.username} установил тебе встречу!\nНа: {event_date} {event_time}\nБудь готов!"
            )

        # Новый формат сообщения
        await update.message.reply_text(
            f"✅ Встреча назначена!\n"
            f"⏰ Дата и время встречи: {event_date} {event_time}\n"
            f"Ждём тебя! 🎉"
        )
    except Exception as e:
        await update.message.reply_text("❌ Произошла ошибка при записи встречи. Пожалуйста, попробуйте снова.")
        print(f"Ошибка при записи встречи: {e}")
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

    await start(update, context)
    context.user_data.clear()

# Функция для просмотра расписания
async def view_schedule(update: Update, context: CallbackContext):
    conn = None
    try:
        user = update.effective_user
        current_time = get_moscow_time().strftime('%Y-%m-%d %H:%M')

        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем все будущие события
        if is_admin(user.username):
            # Для администратора показываем все будущие встречи
            cursor.execute("""
                SELECT event_date, event_time, event_name, full_name, username 
                FROM schedule 
                JOIN users ON schedule.user_id = users.id 
                WHERE datetime(event_date || ' ' || event_time) >= datetime(?)
                ORDER BY event_date, event_time ASC
            """, (current_time,))
        else:
            # Для обычного пользователя показываем все встречи, но только его встречи с деталями
            cursor.execute("""
                SELECT event_date, event_time, event_name, full_name, username 
                FROM schedule 
                JOIN users ON schedule.user_id = users.id 
                WHERE datetime(event_date || ' ' || event_time) >= datetime(?)
                ORDER BY event_date, event_time ASC
            """, (current_time,))

        events = cursor.fetchall()

        if not events:
            # Если встреч нет, показываем сообщение и кнопку "Назад"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text("На данный момент нет запланированных встреч.", reply_markup=reply_markup)
            return

        # Группируем события по датам
        grouped_events = {}
        for event in events:
            event_date, event_time, event_name, full_name, username = event

            # Проверяем формат даты
            try:
                # Если дата в формате YYYY-MM-DD, преобразуем в DD.MM.YYYY для отображения
                event_date_obj = datetime.strptime(event_date, '%Y-%m-%d')
                event_date_formatted = event_date_obj.strftime('%d.%m.%Y')
            except ValueError:
                # Если дата уже в формате DD.MM.YYYY, оставляем как есть
                event_date_formatted = event_date

            if event_date_formatted not in grouped_events:
                grouped_events[event_date_formatted] = []

            # Если пользователь администратор или это его встреча, показываем детали
            if is_admin(user.username) or username == user.username:
                grouped_events[event_date_formatted].append((event_time, event_name, full_name, username))
            else:
                # Иначе показываем "❌ Занято"
                grouped_events[event_date_formatted].append((event_time, "❌ Занято", None, None))

        # Преобразуем строки дат в объекты datetime для корректной сортировки
        sorted_dates = sorted(
            grouped_events.keys(),
            key=lambda x: datetime.strptime(x, "%d.%m.%Y")
        )

        # Разбиваем события на страницы (по 5 дат на страницу)
        page_size = 5
        total_pages = (len(sorted_dates) // page_size + (1 if len(sorted_dates) % page_size != 0 else 0))

        # Получаем текущую страницу из контекста (по умолчанию 1)
        page = context.user_data.get('schedule_page', 1)

        # Ограничиваем страницы в пределах допустимого диапазона
        page = max(1, min(page, total_pages))

        # Выбираем события для текущей страницы
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        dates_to_show = sorted_dates[start_index:end_index]

        # Формируем текст расписания
        schedule_text = "📅 Расписание встреч:\n\n"
        for date in dates_to_show:
            schedule_text += f"📅 {date}\n"
            for event_time, event_name, full_name, username in grouped_events[date]:
                if full_name and username:
                    schedule_text += f"⏰ {event_time} 👤 {full_name} (@{username}) 📝 {event_name}\n"
                else:
                    schedule_text += f"⏰ {event_time} ❌ Занято\n"
            schedule_text += "\n"

        # Добавляем информацию о странице
        schedule_text += f"Страница {page} из {total_pages}"

        # Создаем клавиатуру для навигации
        keyboard = []
        if page > 1:
            keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f'schedule_page_{page - 1}')])
        if page < total_pages:
            keyboard.append([InlineKeyboardButton("Вперед ➡️", callback_data=f'schedule_page_{page + 1}')])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='back')])

        reply_markup = InlineKeyboardMarkup(keyboard)

        # Редактируем текущее сообщение
        await update.callback_query.edit_message_text(schedule_text, reply_markup=reply_markup)

        # Сохраняем текущую страницу в контексте
        context.user_data['schedule_page'] = page
        update_last_active(context)
    except Exception as e:
        logger.error(f"Ошибка в view_schedule: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# Функция для просмотра своих встреч
async def my_events(update: Update, context: CallbackContext):
    user = update.effective_user
    current_time = get_moscow_time().strftime('%Y-%m-%d %H:%M')

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем встречи текущего пользователя
#        cursor.execute("""
#            SELECT event_date, event_time, event_name 
#            FROM schedule 
#            WHERE user_id = ? AND datetime(substr(event_date, 7, 4) || '-' || substr(event_date, 4, 2) || '-' || substr(event_date, 9, 2) || ' ' || event_time) >= datetime(?)
#            ORDER BY event_date, event_time
#        """, (user.id, current_time))
#        events = cursor.fetchall()
        
        cursor.execute("""
            SELECT event_date, event_time, event_name 
            FROM schedule 
            WHERE user_id = ? 
            AND datetime(event_date || ' ' || event_time) >= datetime(?)
            ORDER BY event_date, event_time
        """, (user.id, current_time))
        events = cursor.fetchall()

        logger.info(f"Ищем встречи для user_id={user.id}, current_time={current_time}")
        logger.info(f"Найдено встреч: {len(events)}")
        
        if not events:
            # Если встреч нет, показываем сообщение и кнопку "Назад"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text("📅 На данный момент нет запланированных встреч. \n😊 Хочешь назначить новую?", reply_markup=reply_markup)
            return

        # Формируем текст встреч
        schedule_text = "⏰ Мои встречи: ⏰\n\n"
        current_date = None

        for event in events:
            event_date, event_time, event_name = event
            
            # Преобразуем дату в формат DD.MM.YYYY
            event_date_formatted = datetime.strptime(event_date, '%Y-%m-%d').strftime('%d.%m.%Y')

            # Если дата изменилась, добавляем заголовок с новой датой
            if event_date_formatted != current_date:
                if current_date is not None:
                    schedule_text += "\n"  # Добавляем пустую строку между блоками дат
                schedule_text += f"📅 {event_date_formatted}\n"
                current_date = event_date_formatted

            # Добавляем время и тему встречи
            schedule_text += f"⏰ {event_time} 📝 {event_name}\n"

        # Добавляем кнопку "Назад"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Обновляем сообщение
        await update.callback_query.edit_message_text(schedule_text, reply_markup=reply_markup)
        update_last_active(context)  # Обновляем время последней активности
        
    except Exception as e:
        logger.error(f"Ошибка в my_events: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

# Функция для просмотра прошедших встреч
async def past_events(update: Update, context: CallbackContext):
    conn = None
    try:
        user = update.effective_user
        current_time = get_moscow_time().strftime('%Y-%m-%d %H:%M')

        # Логируем текущего пользователя
        logger.info(f"Текущий пользователь: id={user.id}, username={user.username}")

        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем все прошедшие события
        if is_admin(user.username):
            # Для администратора показываем все прошедшие встречи
            cursor.execute("""
                SELECT event_date, event_time, event_name, full_name, username 
                FROM schedule 
                JOIN users ON schedule.user_id = users.id 
                WHERE datetime(event_date || ' ' || event_time) < datetime(?)
                ORDER BY event_date DESC, event_time ASC
            """, (current_time,))
        else:
            # Для обычного пользователя показываем только его прошедшие встречи
            cursor.execute("""
                SELECT event_date, event_time, event_name, user_id 
                FROM schedule 
                WHERE datetime(event_date || ' ' || event_time) < datetime(?)
                ORDER BY event_date DESC, event_time ASC
            """, (current_time,))

        events = cursor.fetchall()

        if not events:
            # Если встреч нет, показываем сообщение и кнопку "Назад"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text("📅 У тебя пока нет прошедших встреч. \n😉 Но ты всегда можешь запланировать новые!", reply_markup=reply_markup)
            return

        # Группируем события по датам
        grouped_events = {}
        for event in events:
            if is_admin(user.username):
                # Для администратора распаковываем все 5 значений
                event_date, event_time, event_name, full_name, username = event
            else:
                # Для обычного пользователя распаковываем только 3 значения
                event_date, event_time, event_name, event_user_id = event
                full_name, username = None, None  # Заполняем пустыми значениями

            # Проверяем формат даты
            try:
                # Если дата в формате YYYY-MM-DD, преобразуем в DD.MM.YYYY для отображения
                event_date_obj = datetime.strptime(event_date, '%Y-%m-%d')
                event_date_formatted = event_date_obj.strftime('%d.%m.%Y')
            except ValueError:
                # Если дата уже в формате DD.MM.YYYY, оставляем как есть
                event_date_formatted = event_date

            if event_date_formatted not in grouped_events:
                grouped_events[event_date_formatted] = []

            # Если пользователь администратор или это его встреча, показываем детали
            if is_admin(user.username) or (not is_admin(user.username) and event_user_id == user.id):
                grouped_events[event_date_formatted].append((event_time, event_name, full_name, username))
            else:
                grouped_events[event_date_formatted].append((event_time, "❌ Занято", None, None))

        # Преобразуем строки дат в объекты datetime для корректной сортировки
        sorted_dates = sorted(
            grouped_events.keys(),
            key=lambda x: datetime.strptime(x, "%d.%m.%Y"),
            reverse=True
        )

        # Разбиваем события на страницы (по 5 дат на страницу)
        page_size = 5
        total_pages = (len(sorted_dates) // page_size + (1 if len(sorted_dates) % page_size != 0 else 0))

        # Получаем текущую страницу из контекста (по умолчанию 1)
        page = context.user_data.get('past_events_page', 1)

        # Ограничиваем страницы в пределах допустимого диапазона
        page = max(1, min(page, total_pages))

        # Выбираем события для текущей страницы
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        dates_to_show = sorted_dates[start_index:end_index]

        # Формируем текст прошедших встреч
        schedule_text = "⏰ Прошедшие встречи: ⏰\n\n"
        for date in dates_to_show:
            schedule_text += f"📅 {date}\n"
            for event in grouped_events[date]:
                if is_admin(user.username):
                    # Для администратора распаковываем 4 значения
                    event_time, event_name, full_name, username = event
                    schedule_text += f"⏰ {event_time} 👤 {full_name} (@{username}) 📝 {event_name}\n"
                else:
                    # Для обычного пользователя распаковываем 2 значения
                    event_time, event_name, _, _ = event  # Игнорируем лишние значения
                    schedule_text += f"⏰ {event_time} 📝 {event_name}\n"
            schedule_text += "\n"

        # Добавляем информацию о странице
        schedule_text += f"Страница {page} из {total_pages}"

        # Создаем клавиатуру для навигации
        keyboard = []
        if page > 1:
            keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f'past_events_page_{page - 1}')])
        if page < total_pages:
            keyboard.append([InlineKeyboardButton("Вперед ➡️", callback_data=f'past_events_page_{page + 1}')])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data='back')])

        reply_markup = InlineKeyboardMarkup(keyboard)

        # Редактируем текущее сообщение
        await update.callback_query.edit_message_text(schedule_text, reply_markup=reply_markup)

        # Сохраняем текущую страницу в контексте
        context.user_data['past_events_page'] = page
        update_last_active(context)
    except Exception as e:
        logger.error(f"Ошибка в past_events: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# Функция для удаления встречи
async def delete_event(update: Update, context: CallbackContext):
    user = update.effective_user
    meetings = await get_upcoming_meetings(user.id)
    
    if not meetings:
        await update.callback_query.edit_message_text("📅 У тебя пока нет запланированных встреч. 😊")
        return
    
    # Формируем текст с предстоящими встречами
    meetings_text = "❌ Хочешь удалить встречу? \nТвои встречи:\n"
    for i, (event_date, event_time, event_name) in enumerate(meetings, start=1):
        event_date_formatted = datetime.strptime(event_date, '%Y-%m-%d').strftime('%d.%m.%Y')
        meetings_text += f"{i}. {event_date_formatted} {event_time} - {event_name}\n"
    
    meetings_text += "\nВведи дату и время в формате: ДД.ММ.ГГГГ ЧЧ:ММ. \n\nНапример: 15.09.2025 19:00."
    
    await update.callback_query.edit_message_text(meetings_text)
    context.user_data['step'] = 'get_event_to_delete'
    update_last_active(context)

# Обработка ввода для удаления встречи
async def handle_delete_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    user = update.effective_user
    event_datetime = update.message.text

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            event_date, event_time = event_datetime.split()
            event_date = datetime.strptime(event_date, '%d.%m.%Y').strftime('%Y-%m-%d')  # Преобразуем дату в формат базы данных
            event_time = datetime.strptime(event_time, '%H:%M').strftime('%H:%M')  # Преобразуем время в формат базы данных
#            datetime.strptime(event_date, '%d.%m.%Y')
#            datetime.strptime(event_time, '%H:%M')
        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Введи в формате ДД.ММ.ГГГГ ЧЧ:ММ")
            return

        # Логируем данные перед поиском
        logger.info(f"Ищем встречу: event_date={event_date}, event_time={event_time}")

        cursor.execute("SELECT user_id FROM schedule WHERE event_date = ? AND event_time = ?", (event_date, event_time))
        event_user_id = cursor.fetchone()
        
        # Логируем результат поиска
        logger.info(f"Результат поиска: {event_user_id}")
        
        if not event_user_id:
            await update.message.reply_text("❌ Встреча не найдена. Проверь введённые данные.")
            return

        if not is_admin(user.username) and event_user_id[0] != user.id:
            await update.message.reply_text("❌ Это не твоя встреча! Проверь введённые данные =)")
            return

        # Получаем информацию о пользователе, который удаляет встречу
        cursor.execute("SELECT full_name, username FROM users WHERE id = ?", (user.id,))
        user_info = cursor.fetchone()
        full_name, username = user_info if user_info else ("Неизвестный", "Неизвестный")

        # Удаляем встречу
        cursor.execute("DELETE FROM schedule WHERE event_date = ? AND event_time = ?", (event_date, event_time))
        conn.commit()

        # Отправляем уведомление администраторам
        for admin in ADMINS:
            try:
                cursor.execute("SELECT id FROM users WHERE username = ?", (admin,))
                admin_id = cursor.fetchone()
                if admin_id:
                    await context.bot.send_message(
                        chat_id=admin_id[0],
                        text=f"🚨 Пользователь {full_name} (@{username}) удалил встречу на {event_date} в {event_time}! 🚨"
                    )
            except Exception as e:
                print(f"Ошибка при отправке сообщения администратору {admin}: {e}")

        await update.message.reply_text("🗑 Хорошо! Встреча удалена! \nЕсли передумаешь, всегда можно назначить новую. 😉")
        await start(update, context)
        context.user_data.clear()

    except Exception as e:
        logger.error(f"Ошибка в handle_delete_event: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

async def get_upcoming_meetings(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    current_time = get_moscow_time().strftime('%Y-%m-%d %H:%M')
    
    cursor.execute("""
        SELECT event_date, event_time, event_name 
        FROM schedule 
        WHERE user_id = ? AND datetime(event_date || ' ' || event_time) >= datetime(?)
        ORDER BY event_date, event_time
    """, (user_id, current_time))
    
    meetings = cursor.fetchall()
    conn.close()
    return meetings

# Функция для переноса встречи
async def reschedule_event(update: Update, context: CallbackContext):
    user = update.effective_user
    meetings = await get_upcoming_meetings(user.id)
    
    if not meetings:
        await update.callback_query.edit_message_text("📅 У тебя пока нет запланированных встреч. 😊")
        return
    
    # Формируем текст с предстоящими встречами
    meetings_text = "🔄 Хочешь перенести встречу? \nТвои встречи:\n"
    for i, (event_date, event_time, event_name) in enumerate(meetings, start=1):
        event_date_formatted = datetime.strptime(event_date, '%Y-%m-%d').strftime('%d.%m.%Y')
        meetings_text += f"{i}. {event_date_formatted} {event_time} - {event_name}\n"
    
    meetings_text += "\nВведи текущую дату и время в формате: ДД.ММ.ГГГГ ЧЧ:ММ. \n\nНапример: 15.09.2025 19:00."
    
    await update.callback_query.edit_message_text(meetings_text)
    context.user_data['step'] = 'get_event_to_reschedule'
    update_last_active(context)

# Обработка ввода для переноса встречи
async def handle_reschedule_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    user = update.effective_user
    event_datetime = update.message.text

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Разделяем введённые дату и время
            event_date, event_time = event_datetime.split()
            
            # Преобразуем дату из формата ДД.ММ.ГГГГ в YYYY-MM-DD
            event_date_obj = datetime.strptime(event_date, '%d.%m.%Y')
            event_date_formatted = event_date_obj.strftime('%Y-%m-%d')  # Форматируем в YYYY-MM-DD
            
            # Преобразуем время в формат базы данных
            event_time_obj = datetime.strptime(event_time, '%H:%M')
            event_time_formatted = event_time_obj.strftime('%H:%M')  # Форматируем в HH:MM

            # Логируем данные перед поиском
            logger.info(f"Ищем встречу: event_date={event_date_formatted}, event_time={event_time_formatted}")

            # Ищем встречу в базе данных
            cursor.execute("SELECT user_id FROM schedule WHERE event_date = ? AND event_time = ?", 
                           (event_date_formatted, event_time_formatted))
            event_user_id = cursor.fetchone()
            
            # Логируем результат поиска
            logger.info(f"Результат поиска: {event_user_id}")
            
            if not event_user_id:
                await update.message.reply_text("❌ Встреча не найдена. Проверь введённые данные.")
                return

            # Проверяем, что встречу можно перенести (админ или владелец встречи)
            if not is_admin(user.username) and event_user_id['user_id'] != user.id:
                await update.message.reply_text("❌ Это не твоя встреча! Проверь введённые данные =)")
                return

            # Сохраняем старую дату и время в контексте
            context.user_data['old_event_date'] = event_date_formatted
            context.user_data['old_event_time'] = event_time_formatted

            await update.message.reply_text("📅 Введи новую дату и время в формате: ДД.ММ.ГГГГ ЧЧ:ММ. \nНапример: 16.09.2025 20:00.")
            context.user_data['step'] = 'get_new_datetime'
            update_last_active(context)  # Обновляем время последней активности

        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Введи в формате ДД.ММ.ГГГГ ЧЧ:ММ.")
    except Exception as e:
        logger.error(f"Ошибка в handle_reschedule_event: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

# Обработка ввода нового времени
async def handle_new_datetime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    user = update.effective_user
    new_datetime = update.message.text

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Разделяем введённые дату и время
            new_date, new_time = new_datetime.split()
            
            # Преобразуем дату из формата ДД.ММ.ГГГГ в YYYY-MM-DD
            new_date_obj = datetime.strptime(new_date, '%d.%m.%Y')
            new_date_formatted = new_date_obj.strftime('%Y-%m-%d')  # Форматируем в YYYY-MM-DD
            
            # Преобразуем время в формат базы данных
            new_time_obj = datetime.strptime(new_time, '%H:%M')
            new_time_formatted = new_time_obj.strftime('%H:%M')  # Форматируем в HH:MM

            # Получаем старую дату и время из контекста
            old_date = context.user_data['old_event_date']
            old_time = context.user_data['old_event_time']

            # Проверяем, что новая дата и время не заняты
            cursor.execute("SELECT * FROM schedule WHERE event_date = ? AND event_time = ?", 
                           (new_date_formatted, new_time_formatted))
            if cursor.fetchone():
                await update.message.reply_text("❌ На это время уже стоит встреча. Выбери другое время.")
                return

            # Обновляем встречу в базе данных
            cursor.execute("""
                UPDATE schedule
                SET event_date = ?, event_time = ?
                WHERE event_date = ? AND event_time = ?
            """, (new_date_formatted, new_time_formatted, old_date, old_time))
            conn.commit()

            # Отправляем уведомление администраторам
            for admin in ADMINS:
                try:
                    cursor.execute("SELECT id FROM users WHERE username = ?", (admin,))
                    admin_id = cursor.fetchone()
                    if admin_id:
                        await context.bot.send_message(
                            chat_id=admin_id[0],
                            text=f"🚀 Пользователь @{user.username} перенёс встречу:\nБыло:\n📅 Дата: {old_date}\n⏰ Время: {old_time}\nСтало:\n📅 Дата: {new_date_formatted}\n⏰ Время: {new_time_formatted}"
                        )
                except Exception as e:
                    print(f"Ошибка при отправке сообщения администратору {admin}: {e}")

            # Новый формат сообщения
            await update.message.reply_text(
                f"🔄 Хорошо! Встреча перенесена!\n"
                f"⏰ Было: {old_date} {old_time}\n"
                f"⏰ Стало: {new_date_formatted} {new_time_formatted}\n"
                f"Не забудь про неё! 🕒"
            )
            await start(update, context)
            context.user_data.clear()
        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Введи в формате ДД.ММ.ГГГГ ЧЧ:ММ.")
    except Exception as e:
        logger.error(f"Ошибка в handle_new_datetime: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

# Функция для просмотра прогресса
async def view_progress(update: Update, context: CallbackContext):
    user = update.effective_user

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем текущий уровень пользователя
        cursor.execute("SELECT level FROM users WHERE id = ?", (user.id,))
        current_level = cursor.fetchone()[0]

        # Получаем все завершенные уровни
        cursor.execute("SELECT level, completed_at FROM progress WHERE user_id = ? ORDER BY completed_at", (user.id,))
        completed_levels = cursor.fetchall()

        # Получаем дату начала обучения
        cursor.execute("SELECT joined_at FROM users WHERE id = ?", (user.id,))
        joined_at = cursor.fetchone()[0]
        if joined_at:
            days_learning = (datetime.now() - datetime.strptime(joined_at, '%Y-%m-%d')).days
        else:
            days_learning = 0  # Если joined_at отсутствует, считаем дни обучения равными 0

        # Формируем текст прогресса
        progress_text = f"📈 Твой текущий уровень: {current_level}. Продолжай в том же духе! 💪\n\n"
        progress_text += "🎯 Ты уже завершил(а) так много:\n"
        for level, completed_at in completed_levels:
            progress_text += f"{level} {completed_at}\n"
        progress_text += f"\n📅 Ты уже учишься {days_learning} дней! Это круто! 🎉"

        # Добавляем кнопку "Назад"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Редактируем текущее сообщение
        await update.callback_query.edit_message_text(progress_text, reply_markup=reply_markup)
      
    except Exception as e:
        logger.error(f"Ошибка в view_progress: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

# Функция для повышения уровня
async def level_up(update: Update, context: CallbackContext):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("🚀 Хочешь повысить свой уровень? Введи кодовое слово ниже. \nНапример: Бамбук")
    context.user_data['step'] = 'get_level_code'
    update_last_active(context)  # Обновляем время последней активности

# Обработка кодового слова для повышения уровня
async def handle_level_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        user = update.effective_user
        level_code = update.message.text.strip()

        # Логируем кодовое слово
        logger.info(f"Кодовое слово: {level_code}, доступные коды: {list(LEVEL_CODES.keys())}")

        # Проверяем, существует ли введенное кодовое слово
        if level_code not in LEVEL_CODES:
            await update.message.reply_text("❌ Неверное кодовое слово. Попробуй ещё раз. Например: Сосисон.")
            return

        new_level = LEVEL_CODES[level_code]

        # Получаем текущий уровень пользователя
        cursor.execute("SELECT level FROM users WHERE id = ?", (user.id,))
        current_level_result = cursor.fetchone()
        
        if not current_level_result:
            await update.message.reply_text("❌ Пользователь не найден.")
            return

        current_level = current_level_result[0]
        logger.info(f"📈 Твой текущий уровень: {current_level}. Продолжай в том же духе! 💪")
        
        # Проверяем, что новый уровень следующий в порядке LEVEL_ORDER
        current_index = LEVEL_ORDER.index(current_level)
        new_index = LEVEL_ORDER.index(new_level)

        if new_index != current_index + 1:
            await update.message.reply_text(f"🚫 Ты не можешь перейти на уровень {new_level}. Сначала заверши уровень {LEVEL_ORDER[current_index + 1]}!")
            return

        # Проверяем, пытается ли пользователь получить уровень, который у него уже есть
        if new_level == current_level:
            await update.message.reply_text(f"😊 Ты уже на уровне: {current_level}. Учись и расти дальше!")
            return

        # Проверяем, пытается ли пользователь понизить уровень
        current_level_index = list(LEVEL_CODES.values()).index(current_level)
        new_level_index = list(LEVEL_CODES.values()).index(new_level)
        logger.info(f"Индекс текущего уровня: {current_level_index}, индекс нового уровня: {new_level_index}")
        
        if new_level_index < current_level_index:
            await update.message.reply_text(f"🚫 Ты уже на уровне: {current_level}. Не нужно понижать уровень, учись и расти вверх!")
            return

        # Проверяем, был ли уже получен этот уровень
        cursor.execute("SELECT level FROM progress WHERE user_id = ? AND level = ?", (user.id, new_level))
        if cursor.fetchone():
            await update.message.reply_text(f"🚫 Ты уже получал уровень: {new_level}. Учись и расти дальше!")
            return

        # Обновляем уровень пользователя
        cursor.execute("UPDATE users SET level = ? WHERE id = ?", (new_level, user.id))
        cursor.execute("INSERT INTO progress (user_id, level, completed_at) VALUES (?, ?, ?)", (user.id, new_level, datetime.now().strftime('%Y-%m-%d')))
        conn.commit()
        logger.info(f"Уровень пользователя обновлен на: {new_level}")

        # Отправляем сообщение с поздравлением
        reward = LEVEL_REWARDS.get(new_level, "🎉 Поздравляем с новым уровнем! 🎉")
        await update.message.reply_text(
            f"🥳🥳🥳 Поздравляю! 🥳🥳🥳\n"
            f"Ты продвинулся на уровень: {new_level}\n"
            f"{reward}"
        )

        # Очищаем данные и сбрасываем флаг
        context.user_data.clear()
        await start(update, context)
    except Exception as e:
        logger.error(f"Ошибка в handle_level_code: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при повышении уровня.")
        
        # Сбрасываем флаг в случае ошибки
        context.user_data.pop('step', None)
        await start(update, context)
    finally:
        if conn:
            conn.close()
    
# Функция для просмотра прогресса всех пользователей
async def view_all_progress(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем прогресс всех пользователей
        cursor.execute("SELECT username, level, joined_at FROM users")
        users = cursor.fetchall()

        if not users:
            # Если пользователей нет, показываем сообщение и кнопку "Назад"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text("Нет данных для отображения.", reply_markup=reply_markup)
            return

        # Формируем текст прогресса
        progress_text = "👥 Прогресс всех пользователей:\n\n"
        for username, level, joined_at in users:
            cursor.execute("SELECT level, completed_at FROM progress WHERE user_id = (SELECT id FROM users WHERE username = ?) ORDER BY completed_at", (username,))
            completed_levels = cursor.fetchall()
            if joined_at:
                days_learning = (datetime.now() - datetime.strptime(joined_at, '%Y-%m-%d')).days
            else:
                days_learning = 0  # Если joined_at отсутствует, считаем дни обучения равными 0

            progress_text += f"@{username} на уровне: {level}\n"
            progress_text += "Завершенные уровни:\n"
            for lvl, completed_at in completed_levels:
                progress_text += f"{lvl} {completed_at}\n"
            progress_text += f"Учится {days_learning} дней\n\n"

        # Добавляем кнопку "Назад"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Редактируем текущее сообщение
        await update.callback_query.edit_message_text(progress_text, reply_markup=reply_markup)

    except Exception as e:
        logger.error(f"Ошибка в view_all_progress: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

async def broadcast(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    await update.callback_query.edit_message_text("📩 Хочешь разослать следующее сообщение? Введи текст сообщения ниже (можно с вложениями). \nНапример: Привет! Напоминаю о встрече, с ответами на вопросы, завтра в 19:00.")
    context.user_data['step'] = 'get_broadcast_message'
    update_last_active(context)

async def handle_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        # Сохраняем текст сообщения
        context.user_data['broadcast_message'] = update.message.text

        # Сохраняем вложения (если есть)
        if update.message.photo:
            context.user_data['broadcast_attachment'] = {
                'type': 'photo',
                'file_id': update.message.photo[-1].file_id  # Берем самое большое фото
            }
        elif update.message.video:
            context.user_data['broadcast_attachment'] = {
                'type': 'video',
                'file_id': update.message.video.file_id
            }
        elif update.message.document:
            context.user_data['broadcast_attachment'] = {
                'type': 'document',
                'file_id': update.message.document.file_id
            }
        else:
            context.user_data['broadcast_attachment'] = None

        await update.message.reply_text("👥 Кому отправить сообщение? \nВведи: Всем, Администратор, Студент, Выпускник, Интересующийся, Отказник.")
        context.user_data['step'] = 'get_broadcast_recipients'
        update_last_active(context)
    except Exception as e:
        logger.error(f"Ошибка в handle_broadcast_message: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

async def handle_broadcast_recipients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == 'stop':
        await start(update, context)
        return

    recipients = update.message.text.lower()
    valid_recipients = ["всем", "администратор", "студент", "выпускник", "интересующийся", "отказник"]
    if recipients not in valid_recipients:
        await update.message.reply_text("❌ Неверный ввод. Введи: Всем, Администратор, Студент, Выпускник, Интересующийся, Отказник.")
        return

    # Проверяем, существует ли ключ в context.user_data
    if 'broadcast_message' not in context.user_data:
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте снова.")
        return

    context.user_data['recipients'] = recipients
    await update.message.reply_text("🔍 Проверь, всё ли верно. \nЕсли да, введи: Да. \nЕсли хочешь изменить, введи: Изменить. \nЕсли передумал(а), введи: Стоп.")
    context.user_data['step'] = 'confirm_broadcast'
    update_last_active(context)

async def handle_broadcast_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    confirmation = update.message.text.lower()
    if confirmation in ['1', 'да']:
        # Проверяем, существуют ли ключи в context.user_data
        if 'broadcast_message' not in context.user_data or 'recipients' not in context.user_data:
            await update.message.reply_text("❌ Произошла ошибка. Попробуйте снова.")
            return

        message = context.user_data['broadcast_message']
        recipients = context.user_data['recipients']
        attachment = context.user_data.get('broadcast_attachment')

        conn = None  # Инициализируем переменную как None
        try:
            # Устанавливаем соединение с базой данных
            conn = get_db_connection()
            cursor = conn.cursor()

            # Получаем список пользователей для рассылки
            if recipients == 'всем':
                cursor.execute("SELECT id FROM users")
            else:
                cursor.execute("SELECT id FROM users WHERE status = ?", (recipients.capitalize(),))

            users = cursor.fetchall()

            # Отправляем сообщение всем пользователям
            for user in users:
                try:
                    if attachment:
                        if attachment['type'] == 'photo':
                            await context.bot.send_photo(chat_id=user[0], photo=attachment['file_id'], caption=message)
                        elif attachment['type'] == 'video':
                            await context.bot.send_video(chat_id=user[0], video=attachment['file_id'], caption=message)
                        elif attachment['type'] == 'document':
                            await context.bot.send_document(chat_id=user[0], document=attachment['file_id'], caption=message)
                    else:
                        await context.bot.send_message(chat_id=user[0], text=message)
                except Exception as e:
                    logger.error(f"Ошибка при отправке сообщения пользователю {user[0]}: {e}")

            await update.message.reply_text("✅ Сообщение отправлено! Теперь они в курсе! 🎉")
            await start(update, context)
            context.user_data.clear()

        except Exception as e:
            logger.error(f"Ошибка в handle_broadcast_confirmation: {e}", exc_info=True)
        finally:
            # Закрываем соединение с базой данных
            if conn:
                conn.close()

    elif confirmation in ['2', 'изменить']:
        await update.message.reply_text("📩 Введи новое сообщение:")
        context.user_data['step'] = 'get_broadcast_message'
    elif confirmation in ['3', 'стоп']:
        await update.message.reply_text("🚫 Рассылка отменена. Если передумаешь, всегда можно разослать заново. 😉")
        await start(update, context)
        context.user_data.clear()
    else:
        await update.message.reply_text("❌ Неверный ввод. Введи: Да/Изменить/Стоп")

# Управление напоминаниями о встречах
async def manage_meeting_reminders(update: Update, context: CallbackContext):
    await update.callback_query.edit_message_text("За какое время присылать уведомление о назначенных встречах? Введи в формате ЧЧ:ММ:")
    context.user_data['step'] = 'get_reminder_time'
    update_last_active(context)

# Обработка ввода времени для напоминаний
async def handle_reminder_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        reminder_time = update.message.text
        try:
            hours, minutes = map(int, reminder_time.split(':'))
            reminder_minutes = hours * 60 + minutes
            context.user_data['reminder_time'] = reminder_minutes

            # Устанавливаем московское время
            user_tz = MOSCOW_TZ

            # Сохраняем время напоминания для администратора
            user = update.effective_user
            cursor.execute(
                "INSERT OR REPLACE INTO reminders (user_id, reminder_type, reminder_time, reminder_text) VALUES (?, ?, ?, ?)",
                (user.id, 'meeting_reminder', reminder_minutes, "Напоминание о встречах")
            )
            conn.commit()
            
            logger.info(f"Сохраняем время напоминания для администратора {user.id}.")
            
            # В любой функции, где нужно проверить данные
            cursor.execute("SELECT * FROM reminders")
            reminders = cursor.fetchall()
            logger.info(f"Данные из таблицы reminders: {reminders}")
            
            # Удаляем старую задачу, если она есть
            if 'meeting_reminder_job' in context.user_data:
                context.user_data['meeting_reminder_job'].schedule_removal()
            
            # Создаем новую задачу для напоминаний
            context.user_data['meeting_reminder_job'] = context.job_queue.run_repeating(
                send_meeting_reminders,
                interval=60.0,  # Проверка каждую минуту
                first=0.0,
                data={"user_id": user.id, "minutes": reminder_minutes, "timezone": user_tz}
            )
            
            logger.info(f"Создаем новую задачу для напоминаний {user.id}.")
            
            await update.message.reply_text(f"Хорошо, теперь буду присылать тебе уведомления за {reminder_time} до назначенных встреч!")
            await start(update, context)
        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Введи число минут.")
    except Exception as e:
        logger.error(f"Ошибка в handle_reminder_time: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# Функция для настройки уведомлений
async def notification_settings(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    keyboard = [
        [InlineKeyboardButton("☝️ Однократное напоминание", callback_data='set_one_time_reminder'), InlineKeyboardButton("✅ Напоминания о назначенных встречах", callback_data='meeting_reminders')],
        [InlineKeyboardButton("🖐 Регулярное напоминание", callback_data='set_recurring_reminder'), InlineKeyboardButton("❌ Удалить уведомление", callback_data='delete_notification')],
        [InlineKeyboardButton("👀 Посмотреть настроенные уведомления", callback_data='view_notifications')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query = update.callback_query
    await query.edit_message_text(text="🔔 Настройка уведомлений:", reply_markup=reply_markup)
    update_last_active(context)  # Обновляем время последней активности

async def send_meeting_reminders(context: CallbackContext):
    conn = None
    try:
        # Получаем текущее время в московском часовом поясе
        now = get_moscow_time()
        logger.info(f"Текущее время (MSK): {now}")

        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем все напоминания из базы данных
        cursor.execute("""
            SELECT r.user_id, r.reminder_type, r.reminder_time, r.reminder_text, 
                   r.meeting_reminder_minutes, r.user_meeting_reminder_minutes
            FROM reminders r
        """)
        reminders = cursor.fetchall()
        logger.info(f"Данные из базы данных: reminders={reminders}")

        if not reminders:
            logger.info("Нет напоминаний для отправки")
            return

        # Обрабатываем каждое напоминание
        for user_id, reminder_type, reminder_time, reminder_text, meeting_reminder_minutes, user_meeting_reminder_minutes in reminders:
            logger.info(f"Обработка напоминания: user_id={user_id}, reminder_type={reminder_type}")

            if reminder_type == 'user_meeting_reminder':
                # Напоминания о встречах для пользователей
                if user_meeting_reminder_minutes:
                    reminder_time = now + timedelta(minutes=user_meeting_reminder_minutes)
                    cursor.execute("""
                        SELECT s.event_date, s.event_time, s.event_name, u.full_name, u.username
                        FROM schedule s
                        JOIN users u ON s.user_id = u.id
                        WHERE s.user_id = ? AND datetime(s.event_date || ' ' || s.event_time) BETWEEN datetime(?) AND datetime(?)
                        AND s.is_blocked_day = FALSE  # Игнорируем встречи с флагом is_blocked_day
                    """, (user_id, now.strftime('%Y-%m-%d %H:%M'), reminder_time.strftime('%Y-%m-%d %H:%M')))
                    meetings = cursor.fetchall()

                    if meetings:
                        for event_date, event_time, event_name, full_name, username in meetings:
                            # Проверяем, было ли уже отправлено уведомление для этой встречи
                            cursor.execute("""
                                SELECT id FROM sent_reminders 
                                WHERE user_id = ? AND event_date = ? AND event_time = ?
                            """, (user_id, event_date, event_time))
                            if cursor.fetchone():
                                logger.info(f"Уведомление для встречи {event_date} {event_time} уже отправлено пользователю {user_id}")
                                continue

                            # Формируем текст уведомления
                            reminder_text = (
                                f"⏰ Я напоминаю, что через {user_meeting_reminder_minutes} минут будет встреча!\n"
                                f"📅 Дата: {event_date}\n"
                                f"⏰ Время: {event_time}\n"
                                f"📝 Тема: {event_name}\n"
                                f"Подготовься, подключайся с ПК, с вебкамерой и хорошим интернетом! И не забудь включить запись! 😊"
                            )
                            await context.bot.send_message(
                                chat_id=user_id,
                                text=reminder_text
                            )
                            # Сохраняем информацию об отправленном уведомлении
                            cursor.execute("""
                                INSERT INTO sent_reminders (user_id, event_date, event_time)
                                VALUES (?, ?, ?)
                            """, (user_id, event_date, event_time))
                            conn.commit()

            elif reminder_type == 'meeting_reminder':
                # Напоминания о встречах для администраторов
                if meeting_reminder_minutes:
                    reminder_time = now + timedelta(minutes=meeting_reminder_minutes)
                    cursor.execute("""
                        SELECT s.event_date, s.event_time, s.event_name, u.full_name, u.username
                        FROM schedule s
                        JOIN users u ON s.user_id = u.id
                        WHERE datetime(s.event_date || ' ' || s.event_time) BETWEEN datetime(?) AND datetime(?)
                        AND s.is_blocked_day = FALSE  # Игнорируем встречи с флагом is_blocked_day
                    """, (now.strftime('%Y-%m-%d %H:%M'), reminder_time.strftime('%Y-%m-%d %H:%M')))
                    meetings = cursor.fetchall()

                    if meetings:
                        for event_date, event_time, event_name, full_name, username in meetings:
                            # Проверяем, было ли уже отправлено уведомление для этой встречи
                            cursor.execute("""
                                SELECT id FROM sent_reminders 
                                WHERE user_id = ? AND event_date = ? AND event_time = ?
                            """, (user_id, event_date, event_time))
                            if cursor.fetchone():
                                logger.info(f"Уведомление для встречи {event_date} {event_time} уже отправлено администратору {user_id}")
                                continue

                            # Формируем текст уведомления
                            reminder_text = (
                                f"⏰ Я напоминаю, что через {meeting_reminder_minutes} минут с {full_name} (@{username}) будет встреча!\n"
                                f"📅 Дата: {event_date}\n"
                                f"⏰ Время: {event_time}\n"
                                f"📝 Тема: {event_name}\n"
                                f"Подготовься, подключайся с ПК, с вебкамерой и хорошим интернетом! И не забудь сказать 'Приветы, ученики!' 😊"
                            )
                            await context.bot.send_message(
                                chat_id=user_id,
                                text=reminder_text
                            )
                            # Сохраняем информацию об отправленном уведомлении
                            cursor.execute("""
                                INSERT INTO sent_reminders (user_id, event_date, event_time)
                                VALUES (?, ?, ?)
                            """, (user_id, event_date, event_time))
                            conn.commit()

            elif reminder_type == 'one_time':
                # Однократные напоминания
                reminder_datetime = datetime.strptime(reminder_time, '%Y-%m-%d %H:%M')
                reminder_datetime = MOSCOW_TZ.localize(reminder_datetime)

                if now >= reminder_datetime:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"⏰ Напоминаю: {reminder_text}"
                    )
                    cursor.execute("DELETE FROM reminders WHERE user_id = ? AND reminder_time = ?", (user_id, reminder_time))
                    conn.commit()

            elif reminder_type in ['weekdays', 'daily']:
                # Регулярные напоминания
                reminder_time_obj = datetime.strptime(reminder_time, '%H:%M').time()
                current_time_minutes = now.time().hour * 60 + now.time().minute
                reminder_time_minutes = reminder_time_obj.hour * 60 + reminder_time_obj.minute
                time_diff = abs(current_time_minutes - reminder_time_minutes)

                if time_diff == 0:
                    cursor.execute("""
                        SELECT last_sent_date FROM reminders
                        WHERE user_id = ? AND reminder_time = ? AND reminder_type = ?
                    """, (user_id, reminder_time, reminder_type))
                    last_sent_date = cursor.fetchone()

                    if not last_sent_date or last_sent_date[0] != now.date():
                        if reminder_type == 'weekdays' and now.weekday() < 5:
                            await context.bot.send_message(
                                chat_id=user_id,
                                text=f"⏰ Напоминаю: {reminder_text}"
                            )
                        elif reminder_type == 'daily':
                            await context.bot.send_message(
                                chat_id=user_id,
                                text=f"⏰ Напоминаю: {reminder_text}"
                            )

                        cursor.execute("""
                            UPDATE reminders
                            SET last_sent_date = ?
                            WHERE user_id = ? AND reminder_time = ? AND reminder_type = ?
                        """, (now.date(), user_id, reminder_time, reminder_type))
                        conn.commit()

            elif reminder_type == 'test_result':
                # Уведомления о результатах тестов
                cursor.execute("""
                    SELECT u.full_name, u.username, t.result, t.statistics
                    FROM test_results t
                    JOIN users u ON t.user_id = u.id
                    WHERE t.notified = FALSE
                """)
                test_results = cursor.fetchall()

                for full_name, username, result, statistics in test_results:
                    message = (
                        f"📊 Результат теста:\n"
                        f"👤 Пользователь: {full_name} (@{username})\n"
                        f"🎯 Результат: {result}\n"
                        f"📈 Статистика:\n{statistics}"
                    )

                    # Отправляем уведомление администраторам
                    for admin in ADMINS:
                        cursor.execute("SELECT id FROM users WHERE username = ?", (admin,))
                        admin_id = cursor.fetchone()
                        if admin_id:
                            await context.bot.send_message(
                                chat_id=admin_id[0],
                                text=message
                            )

                    # Помечаем уведомление как отправленное
                    cursor.execute("""
                        UPDATE test_results
                        SET notified = TRUE
                        WHERE user_id = (SELECT id FROM users WHERE username = ?)
                    """, (username,))
                    conn.commit()

    except Exception as e:
        logger.error(f"Ошибка при отправке напоминаний: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()


# Установить однократное напоминание
async def set_one_time_reminder(update: Update, context: CallbackContext):
    await update.callback_query.edit_message_text("📅 Хочешь установить напоминание? \nВведи дату и время в формате: ДД.ММ.ГГГГ ЧЧ:ММ. \nНапример: 15.09.2025 19:00.")
    context.user_data['step'] = 'get_one_time_reminder_datetime'
    update_last_active(context)

# Обработка ввода даты и времени для однократного напоминания
async def handle_one_time_reminder_datetime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        reminder_datetime = update.message.text
        try:
            reminder_date, reminder_time = reminder_datetime.split()
            datetime.strptime(reminder_date, '%d.%m.%Y')
            datetime.strptime(reminder_time, '%H:%M')
            
            # Преобразуем в формат YYYY-MM-DD HH:MM
            reminder_datetime_obj = datetime.strptime(f"{reminder_date} {reminder_time}", '%d.%m.%Y %H:%M')
            reminder_datetime_obj = MOSCOW_TZ.localize(reminder_datetime_obj)  # Устанавливаем московское время
            reminder_datetime_str = reminder_datetime_obj.strftime('%Y-%m-%d %H:%M')
            
            context.user_data['reminder_datetime'] = reminder_datetime_str
            await update.message.reply_text("📝 Введи текст напоминания. Например: Не забудь про встречу!")
            context.user_data['step'] = 'get_one_time_reminder_text'
            update_last_active(context)
        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Введи в формате ДД.ММ.ГГГГ ЧЧ:ММ")
    except Exception as e:
        logger.error(f"Ошибка в handle_one_time_reminder_datetime: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# Обработка ввода текста для однократного напоминания
async def handle_one_time_reminder_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        reminder_text = update.message.text
        reminder_datetime_str = context.user_data['reminder_datetime']
        
        # Преобразуем строку обратно в объект datetime
        reminder_datetime_obj = datetime.strptime(reminder_datetime_str, '%Y-%m-%d %H:%M')
        reminder_datetime_obj = MOSCOW_TZ.localize(reminder_datetime_obj)
        
        # Сохраняем напоминание в базу данных
        cursor.execute(
            "INSERT INTO reminders (user_id, reminder_type, reminder_time, reminder_text) VALUES (?, ?, ?, ?)",
            (update.effective_user.id, 'one_time', reminder_datetime_str, reminder_text)
        )
        conn.commit()
        
        # Создаем задачу для однократного напоминания
        context.job_queue.run_once(
            send_one_time_reminder,
            when=reminder_datetime_obj,
            data={"user_id": update.effective_user.id, "reminder_text": reminder_text}
        )

        await update.message.reply_text("🔔 Напоминание установлено! \n😊 Я обязательно напомню тебе об этом!")
        await start(update, context)
        context.user_data.clear()
    except Exception as e:
        logger.error(f"Ошибка в handle_one_time_reminder_text: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# Обработка подтверждения однократного напоминания
async def handle_one_time_reminder_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    confirmation = update.message.text.lower()
    if confirmation in ['1', 'да']:
        user = update.effective_user
        reminder_date = context.user_data['reminder_date']
        reminder_time = context.user_data['reminder_time']
        reminder_text = context.user_data['reminder_text']

        # Сохраняем напоминание в базу данных
        cursor.execute(
            "INSERT INTO reminders (user_id, reminder_type, reminder_time, reminder_text) VALUES (?, ?, ?, ?)",
            (user.id, 'one_time', f"{reminder_date} {reminder_time}", reminder_text)
        )
        conn.commit()

        # Планируем задачу для отправки напоминания
        reminder_datetime = datetime.strptime(f"{reminder_date} {reminder_time}", '%d.%m.%Y %H:%M')
        reminder_datetime = MOSCOW_TZ.localize(reminder_datetime)
        context.job_queue.run_once(
            send_one_time_reminder,
            reminder_datetime,
            data={"user_id": user.id, "reminder_text": reminder_text}
        )

        await update.message.reply_text("🔔 Напоминание установлено! \n😊 Я обязательно напомню тебе об этом!")
        await start(update, context)
        context.user_data.clear()
    elif confirmation in ['2', 'изменить']:
        await update.message.reply_text("📝 Введи текст напоминания. Например: Не забудь про встречу!")
        context.user_data['step'] = 'get_one_time_reminder_text'
    elif confirmation in ['3', 'отменить']:
        await update.message.reply_text("Уведомление отменено.")
        await start(update, context)
        context.user_data.clear()
    else:
        await update.message.reply_text("❌ Неверный ввод. Введи 1) Да, 2) Изменить, 3) Отменить.")

async def send_one_time_reminder(context: CallbackContext):
    conn = None  # Инициализируем переменную как None
    try:
        # Получаем текущее время в московском часовом поясе
        now = get_moscow_time()
        logger.info(f"Текущее время (MSK): {now}")

        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем все однократные напоминания из базы данных
        cursor.execute("""
            SELECT r.user_id, r.reminder_time, r.reminder_text, u.full_name, u.username
            FROM reminders r
            JOIN users u ON r.user_id = u.id
            WHERE r.reminder_type = 'one_time'
        """)
        reminders = cursor.fetchall()
        logger.info(f"Найдено напоминаний: {len(reminders)}")

        if not reminders:
            logger.info("Нет напоминаний для отправки")
            return

        # Обрабатываем каждое напоминание
        for user_id, reminder_time, reminder_text, full_name, username in reminders:
            logger.info(f"Обработка напоминания для пользователя {username} (ID: {user_id})")

            # Преобразуем время из базы данных в объект datetime
            reminder_datetime = datetime.strptime(reminder_time, '%Y-%m-%d %H:%M')
            reminder_datetime = MOSCOW_TZ.localize(reminder_datetime)
            logger.info(f"Время напоминания: {reminder_datetime}")

            # Проверяем, наступило ли время напоминания
            if now >= reminder_datetime:
                logger.info("Время напоминания наступило")
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"⏰ Напоминаю: {reminder_text}\n"
                         f"🕒 Время: {now.strftime('%d.%m.%Y %H:%M')} (MSK)"
                )
                logger.info(f"Напоминание отправлено пользователю {username} (ID: {user_id})")

                # Удаляем напоминание после отправки
                cursor.execute("DELETE FROM reminders WHERE user_id = ? AND reminder_time = ?", (user_id, reminder_time))
                conn.commit()
                logger.info(f"Напоминание удалено из базы данных для пользователя {username} (ID: {user_id})")

    except Exception as e:
        logger.error(f"Ошибка в send_one_time_reminder: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

# Установить повторяющееся напоминание
async def set_recurring_reminder(update: Update, context: CallbackContext):
    await update.callback_query.edit_message_text("⏰ Хочешь установить повторяющееся напоминание? Введи время в формате: ЧЧ:ММ. \nНапример: 19:00.")
    context.user_data['step'] = 'get_recurring_reminder_time'
    update_last_active(context)

# Обработка ввода времени для повторяющегося напоминания
async def handle_recurring_reminder_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        reminder_time = update.message.text
        try:
            # Проверяем формат времени
            datetime.strptime(reminder_time, '%H:%M')  # Проверка формата
            context.user_data['reminder_time'] = reminder_time
            await update.message.reply_text("📅 Повторять по будням (введи 1) или каждый день (введи 2)?")
            context.user_data['step'] = 'get_recurring_reminder_type'
            update_last_active(context)
        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Введи в формате ЧЧ:ММ.")
    except Exception as e:
        logger.error(f"Ошибка в handle_recurring_reminder_time: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# Обработка ввода типа повторяющегося напоминания
async def handle_recurring_reminder_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        reminder_type = update.message.text
        if reminder_type not in ['1', '2']:
            await update.message.reply_text("❌ Неверный ввод. Введи 1 или 2")
            return

        context.user_data['reminder_type'] = 'weekdays' if reminder_type == '1' else 'daily'
        await update.message.reply_text("О чём тебе напомнить? Напиши напоминание:")
        context.user_data['step'] = 'get_recurring_reminder_text'
        update_last_active(context)
    except Exception as e:
        logger.error(f"Ошибка в handle_recurring_reminder_type: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# Обработка ввода текста для повторяющегося напоминания
async def handle_recurring_reminder_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        reminder_text = update.message.text
        reminder_time = context.user_data.get('reminder_time')
        reminder_type = context.user_data.get('reminder_type')

        # Логируем данные перед сохранением
        logger.info(f"Данные для сохранения: reminder_time={reminder_time}, reminder_type={reminder_type}, reminder_text={reminder_text}")

        # Проверяем, что данные в контексте присутствуют
        if None in (reminder_time, reminder_type):
            await update.message.reply_text("❌ Произошла ошибка. Попробуйте снова.")
            return

        # Проверка формата времени
        try:
            reminder_time_obj = datetime.strptime(reminder_time, '%H:%M').time()
        except ValueError:
            await update.message.reply_text("❌ Некорректный формат времени. Попробуйте снова.")
            return

        # Сохраняем напоминание в базу данных
        cursor.execute(
            "INSERT INTO reminders (user_id, reminder_type, reminder_time, reminder_text, last_sent_date) VALUES (?, ?, ?, ?, ?)",
            (update.effective_user.id, reminder_type, reminder_time, reminder_text, None)
        )
        conn.commit()

        # Логируем данные после сохранения
        logger.info(f"Напоминание сохранено: user_id={update.effective_user.id}, reminder_type={reminder_type}, reminder_time={reminder_time}, reminder_text={reminder_text}")

        # Планируем задачу для отправки напоминания
        if reminder_type == 'weekdays':
            context.job_queue.run_daily(
                send_recurring_reminder,
                time=reminder_time_obj,
                days=(0, 1, 2, 3, 4),  # Понедельник-Пятница
                name=f"recurring_reminder_{update.effective_user.id}"  # Уникальный идентификатор задачи
            )
        elif reminder_type == 'daily':
            context.job_queue.run_daily(
                send_recurring_reminder,
                time=reminder_time_obj,
                name=f"recurring_reminder_{update.effective_user.id}"  # Уникальный идентификатор задачи
            )

        await update.message.reply_text("🔔 Повторяющееся напоминание установлено! Я буду напоминать тебе по будням/каждый день! 🕒")
        await start(update, context)
        context.user_data.clear()
    except Exception as e:
        logger.error(f"Ошибка в handle_recurring_reminder_text: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    
# Обработка подтверждения повторяющегося напоминания
async def handle_recurring_reminder_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    confirmation = update.message.text.lower()
    if confirmation in ['1', 'да']:
        user = update.effective_user
        reminder_time = context.user_data['reminder_time']
        reminder_type = context.user_data['reminder_type']
        reminder_text = context.user_data['reminder_text']

        # Сохраняем напоминание в базу данных
        cursor.execute(
            "INSERT INTO reminders (user_id, reminder_type, reminder_time, reminder_text, last_sent_date) VALUES (?, ?, ?, ?, ?)",
            (user.id, reminder_type, reminder_time, reminder_text, None)
        )
        conn.commit()

        # Планируем задачу для отправки напоминания
        if reminder_type == 'weekdays':
            context.job_queue.run_daily(
                send_recurring_reminder,
                time=datetime.strptime(reminder_time, '%H:%M').time(),
                days=(0, 1, 2, 3, 4),  # Понедельник-Пятница
                data={"user_id": user.id, "reminder_text": reminder_text}
            )
        elif reminder_type == 'daily':
            context.job_queue.run_daily(
                send_recurring_reminder,
                time=datetime.strptime(reminder_time, '%H:%M').time(),
                data={"user_id": user.id, "reminder_text": reminder_text}
            )

        await update.message.reply_text("🔔 Повторяющееся напоминание установлено! Я буду напоминать тебе по будням/каждый день! 🕒")
        await start(update, context)
        context.user_data.clear()
    elif confirmation in ['2', 'изменить']:
        await update.message.reply_text("О чём тебе напомнить? Напиши напоминание:")
        context.user_data['step'] = 'get_recurring_reminder_text'
    elif confirmation in ['3', 'отменить']:
        await update.message.reply_text("Уведомление отменено.")
        await start(update, context)
        context.user_data.clear()
    else:
        await update.message.reply_text("❌ Неверный ввод. Введи 1) Да, 2) Изменить, 3) Отменить.")

async def send_recurring_reminder(context: CallbackContext):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Получаем текущее время в московском часовом поясе
        now = get_moscow_time()
        logger.info(f"Текущее время (MSK): {now}")

        # Получаем все регулярные напоминания из базы данных
        cursor.execute("""
            SELECT r.user_id, r.reminder_time, r.reminder_text, r.reminder_type, r.last_sent_date
            FROM reminders r
            WHERE r.reminder_type IN ('weekdays', 'daily')
        """)
        reminders = cursor.fetchall()
        logger.info(f"Данные из базы данных: reminders={reminders}")

        if not reminders:
            logger.info("Нет напоминаний для отправки")
            return

        # Обрабатываем каждое напоминание
        for user_id, reminder_time, reminder_text, reminder_type, last_sent_date in reminders:
            logger.info(f"Обработка напоминания: user_id={user_id}, reminder_time={reminder_time}, reminder_text={reminder_text}, reminder_type={reminder_type}")

            # Преобразуем время из базы данных в объект времени
            reminder_time_obj = datetime.strptime(reminder_time, '%H:%M').time()

            # Проверяем, что разница во времени не превышает 1 минуту
            current_time_minutes = now.time().hour * 60 + now.time().minute
            reminder_time_minutes = reminder_time_obj.hour * 60 + reminder_time_obj.minute
            time_diff = abs(current_time_minutes - reminder_time_minutes)

            if time_diff == 0:
                # Проверяем, было ли уже отправлено напоминание сегодня
                if last_sent_date != now.date():
                    # Если напоминание по будням, проверяем, что сегодня будний день
                    if reminder_type == 'weekdays' and now.weekday() < 5:  # 0-4 = Понедельник-Пятница
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=f"⏰ Напоминаю: {reminder_text}"
                        )
                        # Обновляем дату последней отправки
                        cursor.execute("""
                            UPDATE reminders
                            SET last_sent_date = ?
                            WHERE user_id = ? AND reminder_time = ? AND reminder_type = ?
                        """, (now.date(), user_id, reminder_time, reminder_type))
                        conn.commit()
                        logger.info(f"Напоминание отправлено пользователю {user_id} и обновлена дата последней отправки.")

                    # Если напоминание ежедневное, отправляем без проверки дня
                    elif reminder_type == 'daily':
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=f"⏰ Напоминаю: {reminder_text}"
                        )
                        # Обновляем дату последней отправки
                        cursor.execute("""
                            UPDATE reminders
                            SET last_sent_date = ?
                            WHERE user_id = ? AND reminder_time = ? AND reminder_type = ?
                        """, (now.date(), user_id, reminder_time, reminder_type))
                        conn.commit()
                        logger.info(f"Напоминание отправлено пользователю {user_id} и обновлена дата последней отправки.")
    except Exception as e:
        logger.error(f"Ошибка при отправке регулярного напоминания: {e}")
    finally:
        if conn:
            conn.close()

# Посмотреть настроенные уведомления
async def view_notifications(update: Update, context: CallbackContext):
    user = update.effective_user

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()
        # Получаем все напоминания для пользователя
        cursor.execute("""
            SELECT r.reminder_type, r.reminder_time, r.reminder_text, u.full_name, u.username
            FROM reminders r
            JOIN users u ON r.user_id = u.id
            WHERE r.user_id = ?
        """, (user.id,))
        reminders = cursor.fetchall()

        if not reminders:
            # Если напоминаний нет, показываем сообщение и кнопку "Назад"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='notification_settings')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text("📋 У тебя пока нет настроенных уведомлений. \n😊 Хочешь создать новое?", reply_markup=reply_markup)
            return

        # Группируем напоминания по типам
        one_time_reminders = {}
        weekday_reminders = []
        daily_reminders = []

        for reminder in reminders:
            reminder_type, reminder_time, reminder_text, full_name, username = reminder

            if reminder_type == 'one_time':
                # Разделяем дату и время для однократных напоминаний
                date, time = reminder_time.split()
                if date not in one_time_reminders:
                    one_time_reminders[date] = []
                one_time_reminders[date].append((time, full_name, username, reminder_text))
            elif reminder_type == 'weekdays':
                weekday_reminders.append((reminder_time, full_name, username, reminder_text))
            elif reminder_type == 'daily':
                daily_reminders.append((reminder_time, full_name, username, reminder_text))

        # Формируем текст для вывода
        notifications_text = "🔔 Настроенные уведомления:\n\n"

        # Однократные напоминания
        if one_time_reminders:
            notifications_text += "Однократные:\n"
            for date, reminders_list in one_time_reminders.items():
                notifications_text += f"{date}\n"
                for time, full_name, username, text in reminders_list:
                    notifications_text += f"{time} {full_name} (@{username}) {text}\n"
                notifications_text += "\n"

        # Регулярные напоминания
        if weekday_reminders or daily_reminders:
            notifications_text += "Регулярные:\n"

            if weekday_reminders:
                notifications_text += "По будням:\n"
                for time, full_name, username, text in weekday_reminders:
                    notifications_text += f"{time} {full_name} (@{username}) {text}\n"
                notifications_text += "\n"

            if daily_reminders:
                notifications_text += "Ежедневные:\n"
                for time, full_name, username, text in daily_reminders:
                    notifications_text += f"{time} {full_name} (@{username}) {text}\n"
                notifications_text += "\n"

        # Добавляем кнопку "Назад" для возврата в меню уведомлений
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='notification_settings')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Редактируем текущее сообщение
        await update.callback_query.edit_message_text(notifications_text, reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Ошибка в view_notifications: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

# Удалить уведомление
async def delete_notification(update: Update, context: CallbackContext):
    await update.callback_query.edit_message_text("🗑 Хочешь удалить напоминание? \nВведи 1 - для однократного или 2 - для повторяющегося.")
    context.user_data['step'] = 'get_notification_type_to_delete'
    update_last_active(context)  # Обновляем время последней активности

# Обработка ввода типа уведомления для удаления
async def handle_notification_type_to_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        notification_type = update.message.text
        if notification_type not in ['1', '2']:
            await update.message.reply_text("❌ Неверный ввод. Введи 1 или 2")
            return

        context.user_data['notification_type'] = 'one_time' if notification_type == '1' else 'recurring'
        if notification_type == '1':
            await update.message.reply_text("📅 Какое напоминание ты хочешь удалить? Введи дату и время напоминания в формате: ДД.ММ.ГГГГ ЧЧ:ММ. \nНапример: 15.09.2025 19:00.")
            context.user_data['step'] = 'get_one_time_notification_to_delete'
        else:
            await update.message.reply_text("📅 Какое напоминание ты хочешь удалить? Введи дату и время напоминания в формате: ДД.ММ.ГГГГ ЧЧ:ММ. \nНапример: 15.09.2025 19:00.")
            context.user_data['step'] = 'get_recurring_notification_type_to_delete'
    except Exception as e:
        logger.error(f"Ошибка в handle_notification_type_to_delete: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# Обработка ввода для удаления однократного напоминания
async def handle_one_time_notification_to_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        reminder_datetime = update.message.text
        try:
            reminder_date, reminder_time = reminder_datetime.split()
            datetime.strptime(reminder_date, '%d.%m.%Y')
            datetime.strptime(reminder_time, '%H:%M')
        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Введи в формате ДД.ММ.ГГГГ ЧЧ:ММ")
            return

        user = update.effective_user
        cursor.execute("DELETE FROM reminders WHERE user_id = ? AND reminder_type = 'one_time' AND reminder_time = ?", (user.id, f"{reminder_date} {reminder_time}"))
        conn.commit()

        await update.message.reply_text("🗑 Напоминание удалено! \n😉 Если передумаешь, всегда можно создать новое.")
        await start(update, context)
        context.user_data.clear()
    except Exception as e:
        logger.error(f"Ошибка в handle_one_time_notification_to_delete: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# Обработка ввода для удаления повторяющегося напоминания
async def handle_recurring_notification_type_to_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        reminder_type = update.message.text
        if reminder_type not in ['1', '2']:
            await update.message.reply_text("❌ Неверный ввод. Введи 1 или 2")
            return

        context.user_data['reminder_type'] = 'weekdays' if reminder_type == '1' else 'daily'
        await update.message.reply_text("📅 Какое напоминание ты хочешь удалить? Введи время напоминания в формате: ЧЧ:ММ. \nНапример: 19:00.")
        context.user_data['step'] = 'get_recurring_notification_to_delete'
    except Exception as e:
        logger.error(f"Ошибка в handle_recurring_notification_type_to_delete: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# Обработка ввода для удаления повторяющегося напоминания
async def handle_recurring_notification_to_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        reminder_time = update.message.text
        try:
            datetime.strptime(reminder_time, '%H:%M')
        except ValueError:
            await update.message.reply_text("❌ Неверный формат. Введи в формате ЧЧ:ММ")
            return

        user = update.effective_user
        reminder_type = context.user_data['reminder_type']
        cursor.execute("DELETE FROM reminders WHERE user_id = ? AND reminder_type = ? AND reminder_time = ?", (user.id, reminder_type, reminder_time))
        conn.commit()

        # Удаляем задачу, если она существует
        if 'recurring_reminder_job' in context.user_data:
            context.user_data['recurring_reminder_job'].schedule_removal()

        await update.message.reply_text("Напоминание удалено!")
        await start(update, context)
        context.user_data.clear()
    except Exception as e:
        logger.error(f"Ошибка в handle_recurring_notification_to_delete: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
    
# Функция для просмотра статистики
async def view_statistics(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем статистику по пользователям
        cursor.execute("""
            SELECT full_name, username, joined_at, 
                   (SELECT COUNT(*) FROM schedule WHERE user_id = users.id) AS meetings_count
            FROM users
        """)
        users = cursor.fetchall()

        if not users:
            # Если пользователей нет, показываем сообщение и кнопку "Назад"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.callback_query.edit_message_text("Нет данных для отображения.", reply_markup=reply_markup)
            return

        # Формируем текст статистики
        statistics_text = "📊 Статистика по пользователям:\n\n"
        for full_name, username, joined_at, meetings_count in users:
            if joined_at:
                days_learning = (datetime.now() - datetime.strptime(joined_at, '%Y-%m-%d')).days
            else:
                days_learning = 0
            statistics_text += f"{full_name} (@{username}) обучается {days_learning} дней, встреч: {meetings_count}\n"

        # Добавляем кнопку "Назад"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Редактируем текущее сообщение
        await update.callback_query.edit_message_text(statistics_text, reply_markup=reply_markup)

    except Exception as e:
        logger.error(f"Ошибка в view_statistics: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

async def block_day(update: Update, context: CallbackContext):
    logger.info("Функция block_day вызвана")
    if not is_admin(update.effective_user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    await update.callback_query.edit_message_text("📅 Ты хочешь заблокировать день? Введи дату в формате: ДД.ММ.ГГГГ. \nНапример: 15.09.2025")
    context.user_data['step'] = 'get_block_day_date'
    logger.info(f"Установлен шаг: {context.user_data['step']}")  # Логируем установленный шаг
    update_last_active(context)

async def handle_block_day_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() == 'stop':
        await start(update, context)
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        block_date = update.message.text
        try:
            # Преобразуем введенную дату из формата ДД.ММ.ГГГГ в YYYY-MM-DD
            block_date_obj = datetime.strptime(block_date, '%d.%m.%Y')
            block_date_formatted = block_date_obj.strftime('%Y-%m-%d')  # Форматируем в YYYY-MM-DD
            context.user_data['block_date'] = block_date_formatted  # Сохраняем в формате YYYY-MM-DD
            await update.message.reply_text("📝 Введи причину блокировки. \nНапример: Технические работы.")
            context.user_data['step'] = 'get_block_day_reason'
            update_last_active(context)  # Обновляем время последней активности
        except ValueError:
            await update.message.reply_text("❌ Неверный формат даты. Введи в формате ДД.ММ.ГГГГ.")
    except Exception as e:
        logger.error(f"Ошибка в handle_block_day_date: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

async def handle_block_day_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        reason = update.message.text
        context.user_data['block_reason'] = reason
        block_date = context.user_data['block_date']  # Дата уже в формате YYYY-MM-DD
        await update.message.reply_text(f"🔒 Уверен, что хочешь заблокировать {block_date}, и оставить учеников плакать в уголке? Введи: 1 (Да), 2 (Изменить), 3 (Нет).")
        context.user_data['step'] = 'confirm_block_day'
        update_last_active(context)  # Обновляем время последней активности
    except Exception as e:
        logger.error(f"Ошибка в handle_block_day_reason: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

async def handle_confirm_block_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Проверяем, ввел ли пользователь "STOP"
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return
            
        confirmation = update.message.text.lower()
        if confirmation in ['1', 'да']:
            block_date = context.user_data['block_date']  # Дата уже в формате YYYY-MM-DD
            reason = context.user_data['block_reason']
            user = update.effective_user

            # Проверяем, есть ли уже встречи в этот день
            cursor.execute("SELECT * FROM schedule WHERE event_date = ?", (block_date,))
            if cursor.fetchone():
                await update.message.reply_text("В этот день уже есть встречи! Реши это сначала!")
                await start(update, context)
                context.user_data.clear()
                return

             # Создаем встречи для блокировки с флагом is_blocked_day = TRUE
            times = ["19:00", "20:00", "21:00", "22:00", "23:00"]
            for time in times:
                cursor.execute(
                    "INSERT INTO schedule (user_id, event_name, event_date, event_time, is_blocked_day) VALUES (?, ?, ?, ?, ?)",
                    (user.id, reason, block_date, time, True)  # Добавляем флаг is_blocked_day
                )
            conn.commit()

            await update.message.reply_text(f"🔒 Хорошо! День {block_date} заблокирован! \nЭх! Ученики будут плакать в уголке. 😢!")
            await start(update, context)
            context.user_data.clear()
        elif confirmation in ['2', 'изменить']:
            await update.message.reply_text("Тогда введи дату в формате: ДД.ММ.ГГГГ")
            context.user_data['step'] = 'get_block_day_date'
        elif confirmation in ['3', 'нет']:
            await update.message.reply_text("🚫 Блокировка отменена. \n😉 Если передумаешь, всегда можно заблокировать позже.")
            await start(update, context)
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Неверный ввод. Введи, 1 - Да, 2 - Изменить, 3 - Нет")
    except Exception as e:
        logger.error(f"Ошибка в handle_confirm_block_day: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

async def manage_users(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    keyboard = [
        [InlineKeyboardButton("👥 Список пользователей", callback_data='list_users')],
        [InlineKeyboardButton("👀 Прогресс всех", callback_data='view_all_progress')],
        [InlineKeyboardButton("🛠 Управление статусами", callback_data='manage_statuses')],
        [InlineKeyboardButton("🗑 Удалить пользователя", callback_data='delete_user')],
        [InlineKeyboardButton("🚫 Блокировка пользователей", callback_data='block_user')],
        [InlineKeyboardButton("🔙 Назад", callback_data='back')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(text="👥 Меню пользователей:", reply_markup=reply_markup)

async def list_users(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    conn = None  # Инициализируем переменную как None
    try:
        # Устанавливаем соединение с базой данных
        conn = get_db_connection()
        cursor = conn.cursor()

        # Получаем всех пользователей, сортируем по статусу
        cursor.execute("SELECT full_name, username, status FROM users ORDER BY status")
        users = cursor.fetchall()

        # Группируем пользователей по статусам
        grouped_users = {}
        for full_name, username, status in users:
            if status not in grouped_users:
                grouped_users[status] = []
            grouped_users[status].append((full_name, username))

        # Формируем текст для вывода
        users_text = "👥 Список пользователей:\n\n"
        for status, user_list in grouped_users.items():
            users_text += f"📌 {status}:\n"
            for i, (full_name, username) in enumerate(user_list, start=1):
                users_text += f"{i}. {full_name} (@{username})\n"
            users_text += "\n"

        # Добавляем кнопку "Назад"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.callback_query.edit_message_text(users_text, reply_markup=reply_markup)

    except Exception as e:
        logger.error(f"Ошибка в list_users: {e}", exc_info=True)
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

async def manage_statuses(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    await update.callback_query.edit_message_text("👤 Хочешь изменить статус пользователя? Введи его ник в формате: @Ник_пользователя. \nНапример: @ivanov.")
    context.user_data['step'] = 'get_user_for_status_change'
    update_last_active(context)

async def handle_user_for_status_change(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        username = update.message.text
        if not username.startswith('@'):
            await update.message.reply_text("❌ Ник должен начинаться с символа @. Попробуйте снова.")
            return

        context.user_data['username'] = username
        await update.message.reply_text("🤔 Какой статус ему присвоить? \n📋 Введи новый статус: Администратор, Студент, Выпускник, Интересующийся, Отказник.")
        context.user_data['step'] = 'get_new_status'
        update_last_active(context)
    except Exception as e:
        logger.error(f"Ошибка в handle_user_for_status_change: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

async def handle_new_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        new_status = update.message.text.lower()  # Приводим к нижнему регистру
        username = context.user_data['username']

        # Проверяем, что статус корректен (приводим допустимые значения к нижнему регистру)
        valid_statuses = ["администратор", "студент", "выпускник", "интересующийся", "отказник"]
        if new_status not in valid_statuses:
            await update.message.reply_text("❌ Неверный статус. Введи один из: Администратор, Студент, Выпускник, Интересующийся, Отказник.")
            return

        # Сохраняем статус в исходном регистре (как введено пользователем)
        context.user_data['new_status'] = update.message.text
        await update.message.reply_text(f"🔍 Подтверди изменение статуса {username} на {update.message.text}? Введи: 1 (Да), 2 (Изменить), 3 (Отменить).")
        context.user_data['step'] = 'confirm_status_change'
        update_last_active(context)
    except Exception as e:
        logger.error(f"Ошибка в handle_new_status: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

async def handle_confirm_status_change(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        confirmation = update.message.text.lower()  # Приводим к нижнему регистру
        if confirmation in ['1', 'да']:
            username = context.user_data['username'][1:]  # Убираем @
            new_status = context.user_data['new_status']  # Используем сохраненный статус в исходном регистре

            # Обновляем статус пользователя
            cursor.execute("UPDATE users SET status = ? WHERE username = ?", (new_status, username))
            conn.commit()

            # Отправляем уведомление другим администраторам
            admin_username = update.effective_user.username
            cursor.execute("SELECT full_name FROM users WHERE username = ?", (username,))
            full_name = cursor.fetchone()[0]

            for admin in ADMINS:
                if admin != admin_username:
                    cursor.execute("SELECT id, chat_id FROM users WHERE username = ?", (admin,))
                    admin_data = cursor.fetchone()
                    if admin_data:
                        admin_id, chat_id = admin_data
                        if chat_id:  # Проверяем, что chat_id существует
                            try:
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"Администратор @{admin_username} изменил статус пользователя {full_name} (@{username}) на {new_status}!"
                                )
                                logger.info(f"Уведомление отправлено администратору {admin} (chat_id: {chat_id})")
                            except Exception as e:
                                logger.error(f"Не удалось отправить сообщение администратору {admin} (chat_id: {chat_id}): {e}")
                        else:
                            logger.warning(f"Администратор {admin} не имеет chat_id.")
                    else:
                        logger.error(f"Администратор {admin} не найден в базе данных.")

            await update.message.reply_text(f"✅ @{username} теперь {new_status}! Все в курсе! 🎉")
            await start(update, context)
            context.user_data.clear()
        elif confirmation in ['2', 'изменить']:
            await update.message.reply_text("👤 Хочешь изменить статус пользователя? Введи его ник в формате: @Ник_пользователя. \nНапример: @ivanov.")
            context.user_data['step'] = 'get_user_for_status_change'
        elif confirmation in ['3', 'отменить']:
            await update.message.reply_text("🚫 Изменение статуса отменено. \n😉 Если передумаешь, всегда можно начать заново.")
            await start(update, context)
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Неверный ввод. Введи 1) Да, 2) Изменить, 3) Отменить.")
    except Exception as e:
        logger.error(f"Ошибка в handle_confirm_status_change: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

async def delete_user(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    await update.callback_query.edit_message_text("🗑 Хочешь удалить пользователя? Введи его ник в формате: @Ник_пользователя. \nНапример: @ivanov.")
    context.user_data['step'] = 'get_user_to_delete'
    update_last_active(context)

async def handle_user_to_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        username = update.message.text
        if not username.startswith('@'):
            await update.message.reply_text("❌ Ник должен начинаться с символа @. Попробуйте снова.")
            return

        # Проверяем, существует ли пользователь
        cursor.execute("SELECT id FROM users WHERE username = ?", (username[1:],))
        user_id = cursor.fetchone()

        if not user_id:
            await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
            await start(update, context)
            return

        context.user_data['username'] = username
        await update.message.reply_text(f"🔍Ты точно хочешь удалить все данные о пользователе {username}? Введи: 1 (Да), 2 (Отменить).")
        context.user_data['step'] = 'confirm_user_deletion'
    except Exception as e:
        logger.error(f"Ошибка в handle_user_to_delete: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

async def handle_confirm_user_deletion(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        confirmation = update.message.text.lower()
        if confirmation in ['1', 'да']:
            username = context.user_data['username'][1:]  # Убираем @

            # Проверяем, существует ли пользователь
            cursor.execute("SELECT id, full_name FROM users WHERE username = ?", (username,))
            user_data = cursor.fetchone()

            if not user_data:
                await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
                await start(update, context)
                context.user_data.clear()
                return

            user_id, full_name = user_data

            # Удаляем пользователя из базы данных
            cursor.execute("DELETE FROM users WHERE username = ?", (username,))
            cursor.execute("DELETE FROM schedule WHERE user_id = ?", (user_id,))
            cursor.execute("DELETE FROM progress WHERE user_id = ?", (user_id,))  # Удаляем прогресс пользователя
            conn.commit()

            # Отправляем уведомление другим администраторам
            admin_username = update.effective_user.username
            for admin in ADMINS:
                if admin != admin_username:
                    cursor.execute("SELECT id, chat_id FROM users WHERE username = ?", (admin,))
                    admin_data = cursor.fetchone()
                    if admin_data:
                        admin_id, chat_id = admin_data
                        if chat_id:  # Проверяем, что chat_id существует
                            try:
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"Администратор @{admin_username} удалил всю информацию о пользователе: {full_name} (@{username})!"
                                )
                                logger.info(f"Уведомление отправлено администратору {admin} (chat_id: {chat_id})")
                            except Exception as e:
                                logger.error(f"Не удалось отправить сообщение администратору {admin} (chat_id: {chat_id}): {e}")
                        else:
                            logger.warning(f"Администратор {admin} не имеет chat_id.")
                    else:
                        logger.error(f"Администратор {admin} не найден в базе данных.")

            await update.message.reply_text(f"🗑 Пользователь @{username} удалён! Все данные стёрты. 🚫!")
            await start(update, context)
            context.user_data.clear()
        elif confirmation in ['2', 'отменить']:
            await update.message.reply_text("🚫 Удаление отменено.\n😉 Если передумаешь, всегда можно удалить позже.")
            await start(update, context)
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Неверный ввод. Введи 1) Да, 2) Отменить.")
    except Exception as e:
        logger.error(f"Ошибка в handle_confirm_user_deletion: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при удалении пользователя. Попробуй ещё раз. 😊")
    finally:
        if conn:
            conn.close()

async def block_user(update: Update, context: CallbackContext):
    if not is_admin(update.effective_user.username):
        await update.callback_query.edit_message_text("🚫 У тебя нет прав для выполнения этой команды. Обратись к администратору. 😊")
        return

    await update.callback_query.edit_message_text("🔒 Хочешь заблокировать или разблокировать пользователя? \nВведи: Заблокировать или Разблокировать.")
    context.user_data['step'] = 'get_block_action'
    update_last_active(context)

async def handle_block_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        action = update.message.text.lower()
        if action not in ['заблокировать', 'разблокировать']:
            await update.message.reply_text("❌ Неверный ввод. Введи Заблокировать или Разблокировать.")
            return

        context.user_data['block_action'] = action
        await update.message.reply_text("🤔Какого пользователя вы хотите заблокировать/разблокировать? \n👤 Введи ник пользователя в формате: @ivan. \nНапример: @ivanov.")
        context.user_data['step'] = 'get_user_to_block'
        update_last_active(context)
    except Exception as e:
        logger.error(f"Ошибка в handle_block_action: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

async def handle_user_to_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if update.message.text.lower() == 'stop':
            await start(update, context)
            return

        username = update.message.text
        if not username.startswith('@'):
            await update.message.reply_text("❌ Ник должен начинаться с символа @. Попробуйте снова.")
            return

        context.user_data['username'] = username
        await update.message.reply_text(f"🔍 Подтверди, что хочешь {context.user_data['block_action']} пользователя {username}? Введи: 1 (Да), 2 (Отменить).")
        context.user_data['step'] = 'confirm_block_action'
        update_last_active(context)
    except Exception as e:
        logger.error(f"Ошибка в handle_user_to_block: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

async def handle_confirm_block_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        confirmation = update.message.text.lower()
        if confirmation in ['1', 'да']:
            username = context.user_data['username'][1:]  # Убираем @
            action = context.user_data['block_action']

            # Проверяем, существует ли пользователь
            cursor.execute("SELECT id, full_name FROM users WHERE username = ?", (username,))
            user_data = cursor.fetchone()

            if not user_data:
                await update.message.reply_text(f"❌ Пользователь @{username} не найден.")
                await start(update, context)
                context.user_data.clear()
                return

            user_id, full_name = user_data

            # Обновляем статус блокировки пользователя
            cursor.execute("UPDATE users SET is_blocked = ? WHERE username = ?", (1 if action == 'заблокировать' else 0, username))
            conn.commit()

            # Отправляем уведомление другим администраторам
            admin_username = update.effective_user.username
            for admin in ADMINS:
                if admin != admin_username:
                    cursor.execute("SELECT id, chat_id FROM users WHERE username = ?", (admin,))
                    admin_data = cursor.fetchone()
                    if admin_data:
                        admin_id, chat_id = admin_data
                        if chat_id:  # Проверяем, что chat_id существует
                            try:
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"Администратор @{admin_username} {action} пользователя: {full_name} (@{username})!"
                                )
                                logger.info(f"Уведомление отправлено администратору {admin} (chat_id: {chat_id})")
                            except Exception as e:
                                logger.error(f"Не удалось отправить сообщение администратору {admin} (chat_id: {chat_id}): {e}")
                        else:
                            logger.warning(f"Администратор {admin} не имеет chat_id.")
                    else:
                        logger.error(f"Администратор {admin} не найден в базе данных.")

            await update.message.reply_text(f"✅ Пользователь @{username}{action}! Все в курсе! 🎉")
            await start(update, context)
            context.user_data.clear()
        elif confirmation in ['2', 'отменить']:
            await update.message.reply_text("🚫 Действие отменено. \n😉 Если передумаешь, всегда можно начать заново.")
            await start(update, context)
            context.user_data.clear()
        else:
            await update.message.reply_text("❌ Неверный ввод. Введи 1) Да, 2) Отменить.")
    except Exception as e:
        logger.error(f"Ошибка в handle_confirm_block_action: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка при блокировке пользователя. Попробуй ещё раз. 😊")
    finally:
        if conn:
            conn.close()

# Обработчик любого текста
async def handle_text(update: Update, context: CallbackContext):
    # Создаем локальное соединение с базой данных
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        user = update.message.from_user
        text = update.message.text.lower()

        # Обработка команды "stop"
        if update.message.text.lower() == 'stop':
            context.user_data.clear()  # Очищаем данные контекста
            await stop_handler(update, context)
            return
            

        # Логируем ввод пользователя
        user = update.effective_user
        text = update.message.text
        logger.info(f"Пользователь {user.username} ввел: {text}")

        # Обновляем chat_id администратора, если он отправил сообщение
        cursor.execute('''
            UPDATE users SET chat_id = ? WHERE username = ?
        ''', (user.id, user.username))
        conn.commit()
        # Логируем обновление chat_id
        logger.info(f"Обновлен chat_id для пользователя {user.username} (chat_id: {user.id}).")    

        # Проверяем, находится ли бот в группе
        if update.message.chat.type in ['group', 'supergroup']:
            # Проверяем, содержит ли сообщение упоминание бота
            if context.bot.username.lower() not in text:
                return  # Игнорируем сообщение, если бот не упомянут

        # Если это личный чат, обрабатываем все сообщения
        if update.message.chat.type == 'private':
            # Обработка текста
            pass  # Продолжаем выполнение функции

        # Если это группа, проверяем упоминание
        elif update.message.chat.type in ['group', 'supergroup']:
            if context.bot.username.lower() not in text:
                return  # Игнорируем сообщение, если бот не упомянут
            # Обработка текста
            pass  # Продолжаем выполнение функции

        # Проверяем, ожидается ли ввод кодового слова
        if context.user_data.get('step') == 'get_level_code':
            await handle_level_code(update, context)
            return

        # Проверяем, ожидается ли добавление вопроса
        if context.user_data.get('awaiting_add_question'):
            await handle_add_question(update, context)
            return

        # Проверяем, ожидается ли массовое добавление вопросов
        if context.user_data.get('awaiting_bulk_add_questions'):
            await handle_bulk_add_questions(update, context)
            return

        # Проверяем, ожидается ли изменение уровня пользователя
        if context.user_data.get('awaiting_change_user_level'):
            await handle_change_user_level(update, context)
            return

        # Проверяем, ожидается ли удаление вопроса
        if context.user_data.get('awaiting_delete_question'):
            await handle_delete_question(update, context)
            return

        # Проверяем, ожидается ли редактирование вопроса
        if context.user_data.get('awaiting_edit_question'):
            await handle_edit_question(update, context)
            return

        # Обработка шагов из первого бота
        if 'step' in context.user_data:
            step = context.user_data['step']
            logger.info(f"Текущий шаг: {step}")

            if step == 'get_full_name':
                await handle_full_name(update, context)    
            elif step == 'get_event_date':
                await handle_event_date(update, context)
            elif step == 'get_event_time':
                await handle_event_time(update, context)
            elif step == 'get_event_topic':
                await handle_event_topic(update, context)
            elif step == 'get_event_to_delete':
                await handle_delete_event(update, context)
            elif step == 'get_event_to_reschedule':
                await handle_reschedule_event(update, context)
            elif step == 'get_new_datetime':
                await handle_new_datetime(update, context)
            elif step == 'get_level_code':
                await handle_level_code(update, context)
            elif step == 'get_broadcast_message':
                await handle_broadcast_message(update, context)
            elif step == 'get_broadcast_recipients':
                await handle_broadcast_recipients(update, context)
            elif step == 'confirm_broadcast':
                await handle_broadcast_confirmation(update, context)
            elif step == 'get_reminder_time':
                await handle_reminder_time(update, context)
            elif step == 'get_one_time_reminder_datetime':
                await handle_one_time_reminder_datetime(update, context)
            elif step == 'get_one_time_reminder_text':
                await handle_one_time_reminder_text(update, context)
            elif step == 'confirm_one_time_reminder':
                await handle_one_time_reminder_confirmation(update, context)
            elif step == 'get_recurring_reminder_time':
                await handle_recurring_reminder_time(update, context)
            elif step == 'get_recurring_reminder_type':
                await handle_recurring_reminder_type(update, context)
            elif step == 'get_recurring_reminder_text':
                await handle_recurring_reminder_text(update, context)
            elif step == 'confirm_recurring_reminder':
                await handle_recurring_reminder_confirmation(update, context)
            elif step == 'get_notification_type_to_delete':
                await handle_notification_type_to_delete(update, context)
            elif step == 'get_one_time_notification_to_delete':
                await handle_one_time_notification_to_delete(update, context)
            elif step == 'get_recurring_notification_type_to_delete':
                await handle_recurring_notification_type_to_delete(update, context)
            elif step == 'get_recurring_notification_to_delete':
                await handle_recurring_notification_to_delete(update, context)
            elif step == 'get_block_day_date':
                await handle_block_day_date(update, context)
            elif step == 'get_block_day_reason':
                await handle_block_day_reason(update, context)
            elif step == 'confirm_block_day':
                await handle_confirm_block_day(update, context)
            elif step == 'get_user_for_status_change':
                await handle_user_for_status_change(update, context)
            elif step == 'get_new_status':
                await handle_new_status(update, context)
            elif step == 'confirm_status_change':
                await handle_confirm_status_change(update, context)
            elif step == 'get_user_to_delete':
                await handle_user_to_delete(update, context)
            elif step == 'confirm_user_deletion':
                await handle_confirm_user_deletion(update, context)
            elif step == 'get_block_action':
                await handle_block_action(update, context)
            elif step == 'get_user_to_block':
                await handle_user_to_block(update, context)
            elif step == 'confirm_block_action':
                await handle_confirm_block_action(update, context)
            elif step == 'get_help_message':
                await handle_help_message(update, context)
            elif step == 'get_user_meeting_reminder_time':
                await handle_user_meeting_reminder_time(update, context)
            elif step == 'get_meeting_reminder_time':
                await handle_meeting_reminder_time(update, context)
            elif step == 'get_broadcast_recipients':
                await handle_broadcast_recipients(update, context)
            elif step == 'get_user_full_name':
                await handle_user_full_name(update, context)
            elif step == 'get_user_username':
                await handle_user_username(update, context)

            # Если шаг не распознан
            else:
                logger.warning(f"Неизвестный шаг: {step}")
                await update.message.reply_text("❌  Произошла ошибка. Пожалуйста, попробуйте снова.")
                await start(update, context)
                context.user_data.clear()
        else:
            # Если нет активного шага, отправляем основное меню
            await start(update, context)
    except Exception as e:
        logger.error(f"Ошибка в handle_text: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла ошибка. Пожалуйста, попробуйте снова.")
    finally:
        # Закрываем соединение с базой данных
        if conn:
            conn.close()

# Определяем функцию handle_mention ДО main()
async def handle_mention(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Проверяем, упомянут ли бот в сообщении
    if context.bot.username.lower() in update.message.text.lower():
        # Сохраняем ID пользователя, который вызвал команду
        context.user_data['user_id'] = update.effective_user.id
        # Отправляем меню
        await start(update, context)

# Определяем функцию error_handler ДО main()
async def error_handler(update: Update, context: CallbackContext):
    logger.error(f"Ошибка: {context.error}")

    # Создаем клавиатуру с кнопкой "Назад"
    keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data='back')]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Формируем сообщение об ошибке
    error_message = (
        "Произошла ошибка. Пожалуйста, попробуйте снова.\n\n"
        f"Ошибка: {context.error}"
    )

    if update.callback_query:
        try:
            await update.callback_query.answer()  # Ответить на callback-запрос
            await update.callback_query.edit_message_text(
                text="❌  Произошла ошибка. Пожалуйста, попробуйте снова.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data='back')]])
            )
        except Exception as e:
            logger.error(f"Ошибка при обработке ошибки: {e}")
    else:
        await update.message.reply_text("❌  Произошла ошибка. Пожалуйста, попробуйте снова.")

    # Логируем ошибку для дальнейшего анализа
    logger.error(f"Ошибка в обработчике: {context.error}", exc_info=True)
        
# Основная функция
def main():
    application = Application.builder().token("токен_вставлять_сюда").build() #Вставь сюда свой токен

    # Регистрируем администраторов при запуске
    application.job_queue.run_once(register_admins, when=0)

    # Обработчики команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(CommandHandler("stop", stop_command))

    # Обработчик упоминаний в группе
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, handle_mention))

    # Обработчики для напоминаний
    application.add_handler(CallbackQueryHandler(set_meeting_reminder_time, pattern='^set_meeting_reminder_time$'))
    application.add_handler(CallbackQueryHandler(set_user_meeting_reminder_time, pattern='^set_user_meeting_reminder_time$'))

    # Получаем job_queue для планирования задач
    job_queue = application.job_queue

    # Задача для проверки тайм-аута
    job_queue.run_repeating(
        check_state_timeout,
        interval=60.0,
        first=0.0,
        name="check_state_timeout"  # Уникальный идентификатор задачи
    )

    # Задача для отправки напоминаний о встречах
    job_queue.run_repeating(
        send_meeting_reminders,
        interval=60.0,
        first=0.0,
        name="send_meeting_reminders"  # Уникальный идентификатор задачи
    )

    # Задача для отправки однократных напоминаний
    job_queue.run_repeating(
        send_one_time_reminder,
        interval=60.0,
        first=0.0,
        name="send_one_time_reminder"  # Уникальный идентификатор задачи
    )

    # Обработчик текстовых сообщений (должен быть последним!)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Обработчики ошибок
    application.add_error_handler(error_handler)

    # Запуск бота
    application.run_polling()
    
if __name__ == '__main__':
    main()