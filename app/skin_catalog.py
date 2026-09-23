SKIN_CATALOG = [
    {
        "id": "cat_before_after",
        "title": "До / після кави",
        "filename": "skin_cat_before_after.png",
        "achievement": "Перший ковток",
        "metric": "total_cups",
        "target": 10,
        "card_color": "#E2A42D",
        "foreground_color": "#1E1E1E",
    },
    {
        "id": "corgi_coffee",
        "title": "Кавовий коргі",
        "filename": "skin_corgi_coffee.png",
        "achievement": "Кавоман",
        "metric": "total_cups",
        "target": 50,
        "card_color": "#7A513C",
        "foreground_color": "#FFFFFF",
    },
    {
        "id": "owl_library",
        "title": "Нічний знавець",
        "filename": "skin_owl_library.png",
        "achievement": "Кавовий знавець",
        "metric": "total_cups",
        "target": 100,
        "card_color": "#49382F",
        "foreground_color": "#FFFFFF",
    },
    {
        "id": "cat_boss",
        "title": "Кавовий бос",
        "filename": "skin_cat_boss.png",
        "achievement": "Кавовий майстер",
        "metric": "total_cups",
        "target": 200,
        "card_color": "#34251F",
        "foreground_color": "#FFFFFF",
    },
    {
        "id": "astronaut",
        "title": "Кава поза Землею",
        "filename": "skin_astronaut.png",
        "achievement": "Кавовий мандрівник",
        "metric": "shops_count",
        "target": 5,
        "card_color": "#263A59",
        "foreground_color": "#FFFFFF",
    },
    {
        "id": "autumn_cat_husky",
        "title": "Осінній дослідник",
        "filename": "skin_autumn_cat_husky.png",
        "achievement": "Дослідник міста",
        "metric": "shops_count",
        "target": 10,
        "card_color": "#70432D",
        "foreground_color": "#FFFFFF",
    },
    {
        "id": "sleepy_bulldog",
        "title": "Ще одну каву...",
        "filename": "skin_sleepy_bulldog.png",
        "achievement": "Майже безкоштовно",
        "metric": "almost_free",
        "target": 6,
        "card_color": "#C58A52",
        "foreground_color": "#1E1E1E",
    },
    {
        "id": "frog_breakfast",
        "title": "Перший бонус",
        "filename": "skin_frog_breakfast.png",
        "achievement": "Перший бонус",
        "metric": "total_free",
        "target": 1,
        "card_color": "#668551",
        "foreground_color": "#FFFFFF",
    },
    {
        "id": "winter_penguin",
        "title": "Теплий бонус",
        "filename": "skin_winter_penguin.png",
        "achievement": "Колекціонер бонусів",
        "metric": "total_free",
        "target": 5,
        "card_color": "#55718A",
        "foreground_color": "#FFFFFF",
    },
    {
        "id": "raccoon_barista",
        "title": "Майстер еспресо",
        "filename": "skin_raccoon_barista.png",
        "achievement": "Мисливець за подарунками",
        "metric": "total_free",
        "target": 10,
        "card_color": "#51382A",
        "foreground_color": "#FFFFFF",
    },
    {
        "id": "sloth_iced",
        "title": "На кофеїні",
        "filename": "skin_sloth_iced.png",
        "achievement": "Серія бонусів",
        "metric": "total_free",
        "target": 3,
        "card_color": "#805D47",
        "foreground_color": "#FFFFFF",
    },
    {
        "id": "brainstorm_team",
        "title": "Кавові генії",
        "filename": "skin_brainstorm_team.png",
        "achievement": "Легенда кави",
        "metric": "total_cups",
        "target": 300,
        "card_color": "#272727",
        "foreground_color": "#FFFFFF",
    },
    {
        "id": "coffee_chaos_girl",
        "title": "Кава. Хаос. Легенда.",
        "filename": "skin_coffee_chaos_girl.png",
        "achievement": "Легенда кави",
        "metric": "total_cups",
        "target": 300,
        "card_color": "#302822",
        "foreground_color": "#FFFFFF",
    },
]


def get_skin_by_id(skin_id: str):
    clean_id = (skin_id or "").strip().lower()

    for skin in SKIN_CATALOG:
        if skin["id"] == clean_id:
            return skin

    return None


def skin_progress(
    skin: dict,
    *,
    total_cups: int,
    total_free: int,
    shops_count: int,
):
    metric = skin["metric"]
    target = skin["target"]

    if metric == "total_cups":
        current = total_cups

    elif metric == "total_free":
        current = total_free

    elif metric == "shops_count":
        current = shops_count

    elif metric == "almost_free":
        # Повторяем текущую логику достижения в iOS:
        # после 6 чашек оно считается открытым.
        current = min(total_cups, 6)

    else:
        current = 0

    return {
        "current": min(current, target),
        "target": target,
        "unlocked": current >= target,
    }
