GOVERNORATES = {
    "Cairo": {
        "name_ar": "القاهرة",
        "latitude": 30.0444,
        "longitude": 31.2357
    },
    "Giza": {
        "name_ar": "الجيزة",
        "latitude": 30.0131,
        "longitude": 31.2089
    },
    "Alexandria": {
        "name_ar": "الإسكندرية",
        "latitude": 31.2001,
        "longitude": 29.9187
    },
    "Dakahlia": {
        "name_ar": "الدقهلية",
        "latitude": 31.0364,
        "longitude": 31.3807
    },
    "Red Sea": {
        "name_ar": "البحر الأحمر",
        "latitude": 27.2579,
        "longitude": 33.8116
    },
    "Beheira": {
        "name_ar": "البحيرة",
        "latitude": 30.8481,
        "longitude": 30.3436
    },
    "Fayoum": {
        "name_ar": "الفيوم",
        "latitude": 29.3084,
        "longitude": 30.8428
    },
    "Gharbia": {
        "name_ar": "الغربية",
        "latitude": 30.7865,
        "longitude": 31.0004
    },
    "Ismailia": {
        "name_ar": "الإسماعيلية",
        "latitude": 30.5965,
        "longitude": 32.2715
    },
    "Monufia": {
        "name_ar": "المنوفية",
        "latitude": 30.5972,
        "longitude": 30.9876
    },
    "Minya": {
        "name_ar": "المنيا",
        "latitude": 28.1099,
        "longitude": 30.7503
    },
    "Qalyubia": {
        "name_ar": "القليوبية",
        "latitude": 30.1792,
        "longitude": 31.2056
    },
    "New Valley": {
        "name_ar": "الوادي الجديد",
        "latitude": 25.4510,
        "longitude": 30.5460
    },
    "Suez": {
        "name_ar": "السويس",
        "latitude": 29.9668,
        "longitude": 32.5498
    },
    "Aswan": {
        "name_ar": "أسوان",
        "latitude": 24.0889,
        "longitude": 32.8998
    },
    "Assiut": {
        "name_ar": "أسيوط",
        "latitude": 27.1783,
        "longitude": 31.1859
    },
    "Beni Suef": {
        "name_ar": "بني سويف",
        "latitude": 29.0661,
        "longitude": 31.0994
    },
    "Port Said": {
        "name_ar": "بورسعيد",
        "latitude": 31.2653,
        "longitude": 32.3019
    },
    "Damietta": {
        "name_ar": "دمياط",
        "latitude": 31.4175,
        "longitude": 31.8144
    },
    "Sharkia": {
        "name_ar": "الشرقية",
        "latitude": 30.5877,
        "longitude": 31.5020
    },
    "South Sinai": {
        "name_ar": "جنوب سيناء",
        "latitude": 29.3102,
        "longitude": 34.1532
    },
    "Kafr El Sheikh": {
        "name_ar": "كفر الشيخ",
        "latitude": 31.1107,
        "longitude": 30.9388
    },
    "Matrouh": {
        "name_ar": "مطروح",
        "latitude": 31.3543,
        "longitude": 27.2373
    },
    "Luxor": {
        "name_ar": "الأقصر",
        "latitude": 25.6872,
        "longitude": 32.6396
    },
    "Qena": {
        "name_ar": "قنا",
        "latitude": 26.1551,
        "longitude": 32.7160
    },
    "North Sinai": {
        "name_ar": "شمال سيناء",
        "latitude": 30.5965,
        "longitude": 32.2715
    },
    "Sohag": {
        "name_ar": "سوهاج",
        "latitude": 26.5591,
        "longitude": 31.6957
    }
}


def get_governorate(name):
    return GOVERNORATES.get(name)


def get_all_governorates():
    return GOVERNORATES
