from scripts.lrw_cache_store import *
def pii_remover(cols):
    pii_cols = {
        "tracking":["ttd_device_id","geo_zip","ttd_user_id","latitude","longitude","xandr_user_id","latitude","longitude","xandr_gender","xandr_user_ip","xandr_age","xandr_device_id","xandr_user_agent"],
        "video":[],
        "imp":["UserAgent","IPAddress","TDID","Latitude","Longitude","DeviceID","ZipCode"],
        "click":["IPAddress","TDID","DeviceID"]
    }
    if isinstance(cols,list):
        cols[:] = [x for x in cols if x not in pii_cols["tracking"] and x not in pii_cols["imp"] and x not in pii_cols["click"]]
    elif isinstance(cols,dict):
        for x in cols.keys():
            if x in pii_cols["tracking"] or x in pii_cols["imp"] or x in pii_cols["click"]:
                cols.pop(x)
    return cols

def columns_info():
    for table_name,params in read_from_cache_store().items():
        print("Cache Table Name ",table_name)
        print("Cached columns")
        print(params["cached_cols"])

def available_cols():
    print("Tracking" )
    print("Available columns")
    print(["site_name","type","browser_ts","dsp","auction_id","advertiser_id","campaign_id","line_item_id","creative_id","game_key","browser","platform_os","platform_os_version","device_make","geo_region","geo_country","ttd_environment","device_type","ttd_adformat","node","dropx","dropy","geo_metro","geo_city"])
    print("TTD Impression")
    print("Available columns")
    print(["AudienceID","AdFormat","Frequency","Site","referrercategorieslist","foldposition","UserHourOfWeek","Country","Region","Metro","City","DeviceType","OSFamily","OS","Browser","Recency","PartnerCostInUSD","DeviceMake", "DeviceModel","RenderingContext","PartnerCurrencyExchangeRateFromUSD","AdvertiserCurrencyExchangeRateFromUSD","AuctionType","AdvertiserId","AdGroupId","ImpressionId", "date"])
    print("TTD video")
    print(["ReferrerUrl",  "RedirectUrl",  "ChannelId",  "DisplayImpressionId",  "Keyword",  "ProcessedTime,KeywordId",  "MatchType",  "DistributionNetwork",  "RawUrl", "date"])
    print("Available columns")
    print("TTD Click")
    print("Available columns")
    print(["CreativeIsTrackable",  "CreativeWasViewable",  "VideoEventComplete",  "PostImpressionEventType",  "ImpressionId", "date"])



