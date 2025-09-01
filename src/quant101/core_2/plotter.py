import matplotlib
import seolpyo_mplchart as mc

matplotlib.rcParams["font.family"] = "DejaVu Sans"


def plot_candlestick(df, ticker: str = None, timeframe: str = "1d"):

    if ticker is None:
        print("error, no ticker, the data is whole market")
        return
    elif "," in ticker:
        print("error, no ticker or not a single stock")
        return
    else:
        format_candleinfo_en = """\
        {dt}

        close:      {close}
        rate:        {rate}
        compare: {compare}
        open:      {open}({rate_open})
        high:       {high}({rate_high})
        low:        {low}({rate_low})
        volume:  {volume}({rate_volume})\
        """
        format_volumeinfo_en = """\
        {dt}

        volume:      {volume}
        volume rate: {rate_volume}
        compare:     {compare}\
        """

        class Chart(mc.SliderChart):
            digit_price = 3
            digit_volume = 1

            unit_price = "$"
            unit_volume = "Vol"
            format_ma = "ma{}"
            format_candleinfo = format_candleinfo_en
            format_volumeinfo = format_volumeinfo_en

        c = Chart()
        c.watermark = f"{ticker}-{timeframe}"  # watermark

        c.date = "timestamps"
        c.Open = "open"
        c.high = "high"
        c.low = "low"
        c.close = "close"
        c.volume = "volume"

        c.set_data(df)

        mc.show()  # same as matplotlib.pyplot.show()
