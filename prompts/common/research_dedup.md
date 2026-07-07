**去重与独立性检查**：
- 必须识别 `independent_signals`：真正相对独立、能够单独影响价格的信号。
- 必须识别 `duplicate_signals`：同一事件在 technical/news_macro/youtube/reddit/x、Bull/Bear、Mediator 中重复出现的情况。若非 ETF 公司基本面事实已被 news_macro 吸收，只能按 news_macro 的子信号去重，不得当作独立 fundamental 票数。
- 必须做 `narrative_clusters`：把 YouTube、Reddit、X 或新闻中相同叙事合并，避免把同一个叙事当成多票。
- 不要把 “News -> Sentiment -> Technical” 链式反应当成三个独立证据；除非它们有不同来源、不同机制、不同时间窗口或不同可验证数据。
