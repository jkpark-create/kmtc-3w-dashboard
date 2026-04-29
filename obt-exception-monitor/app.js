const DATA_PATHS = ["../dist/data.json", "../data.json", "/dist/data.json", "data.json"];
const HISTORY_PATHS = ["history.json", "./history.json", "/obt-exception-monitor/history.json"];

const RISK_DEFS = {
  "BSA 속도 부족": {
    meaning: "최근 부킹 증가속도로는 남은 기간 BSA Gap을 채우기 어렵습니다.",
    approach: "회복, 재확보, 선행부킹 후보를 구분하고 부족분은 대체 물량 또는 BSA 조정으로 판단합니다."
  },
  "BSA 속도 부족 구간의 감소 화주": {
    meaning: "속도 부족 구간 안에서 물량이 줄어든 화주입니다.",
    approach: "해당 화주의 회복 가능 TEU와 선적 계획을 먼저 확인합니다."
  },
  "BSA 미달": {
    meaning: "현재 TEU가 배정/목표 선복 대비 낮습니다.",
    approach: "Gap 규모가 큰 구간부터 회복 가능 물량을 확인합니다."
  },
  "BSA 목표 Gap": {
    meaning: "리드타임 트렌드는 정상이나 BSA/목표 선복 자체가 현재 부킹 흐름보다 큽니다.",
    approach: "영업 회복 후보로 채울 수 있는지와 BSA 조정 또는 대체 물량이 필요한지를 분리해 판단합니다."
  },
  "BSA 목표 Gap 구간의 감소 화주": {
    meaning: "트렌드는 정상인 BSA Gap 구간 안에서 물량이 줄어든 화주입니다.",
    approach: "해당 화주 회복 가능 TEU가 BSA Gap 해소에 실제 기여하는지 확인합니다."
  },
  "3W 확보 부족": {
    meaning: "3주전 선행 부킹이 BSA 대비 충분하지 않습니다.",
    approach: "차차주 이후 선적 계획을 조기 확인하고 선행 부킹을 유도합니다."
  },
  "3W/BSA 낮음": {
    meaning: "WOS-3 부킹이 BSA 대비 낮아 선복 예측 리스크가 있습니다.",
    approach: "미유입 주요 화주의 선적 계획과 경쟁사 전환 여부를 확인합니다."
  },
  "부킹속도 부족": {
    meaning: "최근 일별 증가속도가 필요한 증가속도보다 낮습니다.",
    approach: "필요속도와 실제속도 차이가 큰 구간을 먼저 조치합니다."
  },
  "리드타임 트렌드 부족": {
    meaning: "해당 출항 시점(W+1/W+2/W+3)의 포트별 부킹 성숙도보다 낮습니다.",
    approach: "아직 덜 들어와도 정상인 구간과 실제로 늦은 구간을 구분해 조치합니다."
  },
  "리드타임 트렌드 부족 구간의 감소 화주": {
    meaning: "같은 리드타임의 포트 트렌드보다 느린 구간 안에서 물량이 줄어든 화주입니다.",
    approach: "회복 가능 물량과 선행부킹 전환 가능성을 먼저 확인합니다."
  },
  "예상미달": {
    meaning: "현재 속도 기준 최종 TEU가 BSA에 미달할 가능성이 큽니다.",
    approach: "예상 Gap이 큰 구간은 추가 영업 또는 선복 조정 판단이 필요합니다."
  },
  "TEU 감소": {
    meaning: "비교 기준 대비 현재 TEU가 줄었습니다.",
    approach: "감소 화주와 감소 Route를 분리해 원인을 확인합니다."
  },
  "화주수 감소": {
    meaning: "거래 중인 Active 화주 기반이 줄었습니다.",
    approach: "기존 화주의 미선적 사유와 대체 화주 가능성을 확인합니다."
  },
  "화주 이탈": {
    meaning: "기준 기간에는 있었지만 현재 선적이 없는 화주가 있습니다.",
    approach: "이탈 화주를 우선 접촉해 경쟁사/운임/선복 이슈를 확인합니다."
  },
  "이탈": {
    meaning: "기준 기간에는 선적이 있었지만 현재 선적이 없는 화주입니다.",
    approach: "경쟁사 전환, 운임, 선복, 스케줄 이슈를 확인합니다."
  },
  "급감": {
    meaning: "기준 대비 화주 물량이 큰 폭으로 줄었습니다.",
    approach: "감소 사유와 회복 가능한 잔여 물량을 확인합니다."
  },
  "감소": {
    meaning: "기준 대비 화주 물량이 줄었습니다.",
    approach: "담당 화주의 선적 계획 변동 여부를 확인합니다."
  },
  "3W TEU 감소": {
    meaning: "3주전 선행 부킹 TEU가 기준 대비 줄었습니다.",
    approach: "선행 확보가 약한 화주를 중심으로 차주 이후 계획을 확인합니다."
  },
  "3W 화주수 감소": {
    meaning: "3주전 부킹에 참여한 화주 수가 줄었습니다.",
    approach: "조기 부킹하지 않은 반복 화주를 선별합니다."
  },
  "3W 화주 이탈": {
    meaning: "기준 기간의 3W 부킹 화주가 현재 3W 부킹에서 빠졌습니다.",
    approach: "반복 화주 이탈 여부와 후행 부킹 가능성을 확인합니다."
  },
  "3W 이탈": {
    meaning: "기준 기간에는 3주전 부킹이 있었지만 현재 3W 부킹이 없습니다.",
    approach: "차주 이후 선적 계획과 후행 부킹 가능성을 확인합니다."
  },
  "3W 급감": {
    meaning: "3주전 선행 부킹 물량이 큰 폭으로 줄었습니다.",
    approach: "선행 확보가 약해진 화주의 계획을 우선 확인합니다."
  },
  "3W 감소": {
    meaning: "3주전 선행 부킹 물량이 기준보다 줄었습니다.",
    approach: "조기 부킹 회복 가능성을 확인합니다."
  },
  "화주 물량 감소": {
    meaning: "기존 화주의 물량 규모가 줄었습니다.",
    approach: "감소폭이 큰 화주부터 물량 회복 가능성을 확인합니다."
  },
  "핵심 화주 감소": {
    meaning: "구간 영향도가 큰 반복 화주가 이탈하거나 큰 폭으로 감소했습니다.",
    approach: "회복 가능한 화주와 재확보가 필요한 화주를 분리해 우선순위를 정합니다."
  },
  "3W 고수익 감소": {
    meaning: "고수익 화주의 3주전 부킹이 줄었습니다.",
    approach: "수익성 높은 화주를 우선 접촉합니다."
  },
  "3W 확보율 하락": {
    meaning: "현재 물량 대비 3주전 선행 확보 비중이 기준보다 낮아졌습니다.",
    approach: "반복 화주의 조기 부킹 전환 가능성을 확인합니다."
  },
  "3W 취소율": {
    meaning: "3주전 부킹 중 취소 비중이 높아 실제 선적 전환 리스크가 있습니다.",
    approach: "취소 사유와 대체 부킹 가능성을 함께 확인합니다."
  },
  "3W 취소위험": {
    meaning: "선행 부킹 취소가 누적되어 품질 확인이 필요한 화주입니다.",
    approach: "예약 유지 가능성과 취소 원인을 먼저 확인합니다."
  },
  "Late 의존": {
    meaning: "WOS-2 이후 후행 부킹에 의존하는 비중이 높습니다.",
    approach: "조기 부킹 유도와 선복 예측 리스크를 함께 관리합니다."
  },
  "화주 쏠림": {
    meaning: "소수 화주에 물량이 집중되어 변동 리스크가 큽니다.",
    approach: "주요 화주 의존도와 대체 물량 가능성을 확인합니다."
  },
  "신규대형": {
    meaning: "기준 기간에는 없던 대형 물량이 현재 유입되었습니다.",
    approach: "일회성인지 반복 가능 물량인지 확인하고 선복/장비 대응을 점검합니다."
  },
  "급증": {
    meaning: "기준 대비 물량이 급격히 증가했습니다.",
    approach: "추가 물량의 지속 가능성과 운임 조건, 선복 대응 가능성을 확인합니다."
  },
  "3W 신규대형": {
    meaning: "3주전 선행 부킹에 신규 대형 물량이 유입되었습니다.",
    approach: "반복 가능성과 취소 가능성을 함께 점검합니다."
  },
  "물량 변동": {
    meaning: "선택 구간에서 조치가 필요한 물량 변화가 감지되었습니다.",
    approach: "회복, 방어, 대체가 필요한 화주를 구분해 확인합니다."
  }
};

const RISK_GROUP_DEFS = {
  "리드타임/속도": {
    meaning: "현재 부킹 성숙도와 최근 증가속도가 해당 출항 시점 기대치보다 낮습니다.",
    approach: "W+1/W+2/W+3별로 실제 지연인지 정상적인 미성숙인지 먼저 구분합니다."
  },
  "BSA/선복 Gap": {
    meaning: "현재 또는 예상 최종 TEU가 BSA/목표 선복 대비 부족합니다.",
    approach: "회복 후보로 메울 수 있는 Gap과 대체 물량/BSA 조정이 필요한 Gap을 나눕니다."
  },
  "화주 기반 감소": {
    meaning: "기존 화주의 이탈, 급감, Active 화주수 감소가 발생했습니다.",
    approach: "회복 가능 화주와 재확보 대상 화주를 분리해 접촉 우선순위를 정합니다."
  },
  "3W 선행 부킹": {
    meaning: "3주전 선행 부킹 물량 또는 참여 화주가 기준보다 약합니다.",
    approach: "차차주 이후 물량은 조기 부킹 전환과 후행 유입 가능성을 확인합니다."
  },
  "수익/품질": {
    meaning: "고수익 물량 감소, Late 의존, 취소율 등 부킹 품질 리스크가 있습니다.",
    approach: "수익성 높은 화주와 취소 가능성이 큰 부킹을 우선 점검합니다."
  },
  "집중/변동": {
    meaning: "특정 화주 쏠림, 신규 대형 유입, 급증 등 변동성이 큰 상태입니다.",
    approach: "일회성 여부와 선복/장비 대응 가능성을 확인합니다."
  }
};

const RISK_GROUP_EN = {
  "리드타임/속도": "Lead Time / Pace",
  "BSA/선복 Gap": "BSA / Space Gap",
  "화주 기반 감소": "Customer Base Loss",
  "3W 선행 부킹": "3W Advance Booking",
  "수익/품질": "Profit / Quality",
  "집중/변동": "Concentration / Volatility"
};

const RISK_EN = {
  "BSA 속도 부족": {
    label: "BSA Pace Shortfall",
    meaning: "The recent booking pace is not enough to close the remaining BSA gap.",
    approach: "Separate recoverable customers, win-back candidates, and substitute volume or BSA adjustment needs."
  },
  "BSA 속도 부족 구간의 감소 화주": {
    label: "Declining Customer in BSA Pace Risk",
    meaning: "A customer is declining inside a route where the booking pace cannot close the BSA gap.",
    approach: "Check recoverable TEU and near-term shipment plans first."
  },
  "BSA 미달": {
    label: "BSA Shortfall",
    meaning: "Current TEU is below the assigned or target space.",
    approach: "Start with routes where the gap can be recovered by known customers."
  },
  "BSA 목표 Gap": {
    label: "BSA Target Gap",
    meaning: "Lead-time trend is normal, but the BSA or target space is larger than the current booking flow.",
    approach: "Separate customer recovery from substitute volume or BSA adjustment decisions."
  },
  "BSA 목표 Gap 구간의 감소 화주": {
    label: "Declining Customer in BSA Target Gap",
    meaning: "A customer is declining in a route where the trend is normal but the BSA target is still high.",
    approach: "Check whether the recoverable TEU can materially contribute to the BSA gap."
  },
  "리드타임 트렌드 부족": {
    label: "Lead-Time Trend Shortfall",
    meaning: "Booking maturity for this W+1/W+2/W+3 timing is below the port-level benchmark.",
    approach: "Distinguish normal immaturity from a real booking delay before acting."
  },
  "리드타임 트렌드 부족 구간의 감소 화주": {
    label: "Declining Customer in Lead-Time Risk",
    meaning: "A customer declined within a route that is behind the same lead-time benchmark.",
    approach: "Check recovery potential and advance-booking conversion."
  },
  "부킹속도 부족": {
    label: "Booking Pace Shortfall",
    meaning: "Recent daily pickup is below the required daily pickup.",
    approach: "Prioritize routes with the largest gap between required and actual pace."
  },
  "예상미달": {
    label: "Projected Shortfall",
    meaning: "Projected final TEU is likely to miss BSA or the target.",
    approach: "For large projected gaps, decide between sales recovery, substitute volume, and space adjustment."
  },
  "TEU 감소": { label: "TEU Decline", meaning: "Current TEU is below the comparison baseline.", approach: "Separate customer decline from route-level demand change." },
  "화주수 감소": { label: "Active Customer Decline", meaning: "The active customer base has decreased.", approach: "Check missing repeat customers and substitute customer opportunities." },
  "화주 이탈": { label: "Customer Churn", meaning: "Customers active in the baseline are missing now.", approach: "Check competitor switch, rate, space, schedule, or equipment issues." },
  "이탈": { label: "Lost Customer", meaning: "This customer had volume in the baseline but has no current shipment.", approach: "Check win-back feasibility and reason for loss." },
  "급감": { label: "Sharp Decline", meaning: "Customer volume dropped sharply versus baseline.", approach: "Confirm the cause and recoverable residual volume." },
  "감소": { label: "Decline", meaning: "Customer volume decreased versus baseline.", approach: "Confirm shipment-plan changes with the owner." },
  "3W 확보 부족": { label: "3W Booking Shortfall", meaning: "Advance bookings three weeks before sailing are not sufficient.", approach: "Push earlier booking confirmation for W+2 and W+3 cargo." },
  "3W/BSA 낮음": { label: "Low 3W/BSA", meaning: "WOS-3 booking is low against BSA.", approach: "Check missing repeat customers and possible competitor shifts." },
  "3W TEU 감소": { label: "3W TEU Decline", meaning: "3W advance-booked TEU is below baseline.", approach: "Focus on customers with weaker advance booking." },
  "3W 화주수 감소": { label: "3W Customer Decline", meaning: "Fewer customers are booking three weeks ahead.", approach: "Find repeat customers not yet booked early." },
  "3W 화주 이탈": { label: "3W Customer Churn", meaning: "Baseline 3W customers are missing from current 3W bookings.", approach: "Check repeat-customer plans and late booking possibility." },
  "3W 이탈": { label: "3W Lost Customer", meaning: "This customer had 3W booking in the baseline but has none now.", approach: "Check future shipment plans and late-booking possibility." },
  "3W 급감": { label: "3W Sharp Decline", meaning: "Advance-booking volume dropped sharply.", approach: "Confirm plan changes for customers with weakened early booking." },
  "3W 감소": { label: "3W Decline", meaning: "Advance-booking volume is lower than baseline.", approach: "Check whether early booking can be recovered." },
  "화주 물량 감소": { label: "Customer Volume Decline", meaning: "Existing customer volume has decreased.", approach: "Start with customers with the largest recoverable impact." },
  "핵심 화주 감소": { label: "Key Customer Decline", meaning: "A high-impact repeat customer churned or declined sharply.", approach: "Separate recovery candidates from win-back targets." },
  "3W 고수익 감소": { label: "High-Profit 3W Decline", meaning: "High-profit advance-booked volume has decreased.", approach: "Prioritize profitable customers and rate conditions." },
  "3W 확보율 하락": { label: "3W Secured Rate Drop", meaning: "The advance-booked share of current volume has declined.", approach: "Convert repeat customers to earlier booking." },
  "3W 취소율": { label: "3W Cancellation Rate", meaning: "The cancellation share in 3W bookings is high.", approach: "Check cancellation causes and replacement bookings." },
  "3W 취소위험": { label: "3W Cancellation Risk", meaning: "Advance-booking cancellations require quality checks.", approach: "Confirm booking retention and cancellation reasons." },
  "Late 의존": { label: "Late Booking Dependence", meaning: "The route relies heavily on bookings after WOS-2.", approach: "Manage early-booking push and space forecasting risk." },
  "화주 쏠림": { label: "Customer Concentration", meaning: "Volume is concentrated in a small number of customers.", approach: "Check dependency risk and substitute volume options." },
  "신규대형": { label: "New Large Customer", meaning: "A new large volume appeared versus baseline.", approach: "Check whether it is recurring and whether space/equipment can support it." },
  "급증": { label: "Sharp Increase", meaning: "Volume increased sharply versus baseline.", approach: "Confirm sustainability, rate condition, and space/equipment coverage." },
  "3W 신규대형": { label: "New Large 3W Booking", meaning: "A new large advance booking appeared.", approach: "Check recurrence and cancellation risk." },
  "물량 변동": { label: "Volume Change", meaning: "A notable volume change was detected.", approach: "Separate recovery, protection, and substitute-volume actions." }
};

const PRIORITY_HELP = {
  ko: {
    P1: "P1: 즉시 대응 대상입니다. 리드타임 지연, 큰 BSA/목표 Gap, 핵심 화주 감소가 겹쳐 당일 확인이 필요합니다.",
    P2: "P2: 우선 확인 대상입니다. 아직 시간은 있으나 회복/재확보/선행부킹 조치가 필요합니다.",
    P3: "P3: 관찰 대상입니다. 영향이 작거나 참고용 신호입니다."
  },
  en: {
    P1: "P1: Immediate action. Lead-time delay, large BSA/target gap, or key-customer decline requires same-day follow-up.",
    P2: "P2: Priority check. There is still time, but recovery, win-back, or advance-booking action is needed.",
    P3: "P3: Watch. Lower-impact signal for monitoring."
  }
};

const I18N = {
  ko: {
    loading: "OBT 데이터를 분석하는 중입니다.",
    refresh: "Refresh",
    guide: "Guide",
    langToggle: "EN",
    all: "전체",
    high: "High",
    watch: "Watch",
    filters: {
      horizon: "대응",
      priority: "조치범위",
      month: "Month",
      week: "Week",
      origin: "국가",
      pol: "선적지",
      dest: "도착국가",
      dst: "도착포트",
      sales: "영업사원",
      compare: "비교",
      search: "Search"
    },
    options: {
      next3: "차주~3주뒤",
      w1: "차주",
      w2: "차차주",
      w3: "3주뒤",
      custom: "월/주 선택",
      important: "중요구간",
      all: "전체",
      prevMonth: "전월",
      prevWeek: "전주차",
      avg3: "최근 3개월 평균"
    },
    panels: {
      route: "구간별 특이사항",
      sales: "영업사원별 조치",
      shipper: "화주 단위 Action List",
      issue: "리스크 유형"
    },
    headers: {
      route: "구간",
      trend: "트렌드/속도",
      action: "권장 조치",
      judgment: "판단",
      shipper: "화주",
      sales: "영업사원",
      currentTeu: "현재 TEU",
      baseTeu: "기준 TEU",
      delta: "증감"
    },
    labels: {
      baseline: "기준",
      change: "증감",
      impact: "영향",
      shippers: "화주",
      secured: "확보",
      late: "Late",
      bsaNone: "BSA 없음",
      routes: "routes",
      p1: "P1",
      p2: "P2",
      trend: "트렌드",
      customerState: "화주상태",
      impactTeu: "영향",
      actionGap: "관리 Gap"
    }
  },
  en: {
    loading: "Analyzing OBT data.",
    refresh: "Refresh",
    guide: "Guide",
    langToggle: "KR",
    all: "All",
    high: "High",
    watch: "Watch",
    filters: {
      horizon: "Horizon",
      priority: "Scope",
      month: "Month",
      week: "Week",
      origin: "Origin Country",
      pol: "POL",
      dest: "Destination Country",
      dst: "Destination Port",
      sales: "Sales Owner",
      compare: "Compare",
      search: "Search"
    },
    options: {
      next3: "W+1 to W+3",
      w1: "W+1",
      w2: "W+2",
      w3: "W+3",
      custom: "Custom month/week",
      important: "Focus routes",
      all: "All",
      prevMonth: "Previous month",
      prevWeek: "Previous week",
      avg3: "3-month average"
    },
    panels: {
      route: "Route Exceptions",
      sales: "Sales Action Status",
      shipper: "Customer Action List",
      issue: "Risk Types"
    },
    headers: {
      route: "Route",
      trend: "Trend / Pace",
      action: "Recommended Action",
      judgment: "Judgment",
      shipper: "Customer",
      sales: "Sales Owner",
      currentTeu: "Current TEU",
      baseTeu: "Baseline TEU",
      delta: "Delta"
    },
    labels: {
      baseline: "Base",
      change: "Delta",
      impact: "Impact",
      shippers: "cust.",
      secured: "Secured",
      late: "Late",
      bsaNone: "No BSA",
      routes: "routes",
      p1: "P1",
      p2: "P2",
      trend: "Trend",
      customerState: "Customer State",
      impactTeu: "Impact",
      actionGap: "Action Gap"
    }
  }
};

const state = {
  raw: null,
  history: null,
  loadedPath: "",
  rows: [],
  bsaRows: [],
  months: [],
  lang: "ko",
  riskFilter: "all",
  filters: {
    horizon: "w3",
    priority: "important",
    month: "",
    week: "ALL",
    origin: "ALL",
    pol: "ALL",
    dest: "ALL",
    dst: "ALL",
    sales: "ALL",
    compare: "prevMonth",
    query: ""
  }
};

const els = {};

document.addEventListener("DOMContentLoaded", () => {
  cacheElements();
  bindEvents();
  const savedLang = localStorage.getItem("obtExceptionLang");
  if (savedLang === "ko" || savedLang === "en") {
    state.lang = savedLang;
    document.documentElement.lang = state.lang;
  }
  applyLanguage();
  loadData();
});

function cacheElements() {
  [
    "dataMeta", "horizonFilter", "priorityFilter", "monthFilter", "weekFilter", "originFilter", "polFilter",
    "destFilter", "dstFilter", "salesFilter", "compareFilter", "searchInput", "kpiGrid",
    "routeTable", "salesTable", "shipperTable", "issueList", "loading",
    "routeSubtitle", "salesSubtitle", "shipperSubtitle", "issueSubtitle",
    "refreshBtn", "langToggle", "guideBtn", "guideOverlay", "guideLangToggle", "guideClose", "guideTitle", "guideSubtitle", "guideBody"
  ].forEach((id) => {
    els[id] = document.getElementById(id);
  });
}

function bindEvents() {
  ["horizonFilter", "priorityFilter", "monthFilter", "weekFilter", "originFilter", "polFilter", "destFilter", "dstFilter", "salesFilter", "compareFilter"].forEach((id) => {
    els[id].addEventListener("change", () => {
      const key = id.replace("Filter", "");
      state.filters[key] = els[id].value;
      if (id === "monthFilter") {
        state.filters.month = els[id].value;
        state.filters.week = "ALL";
      }
      refreshDependentFilters();
      render();
    });
  });

  els.searchInput.addEventListener("input", debounce(() => {
    state.filters.query = els.searchInput.value.trim().toLowerCase();
    render();
  }, 180));

  els.refreshBtn.addEventListener("click", () => loadData(true));
  els.langToggle.addEventListener("click", toggleLanguage);
  els.guideLangToggle.addEventListener("click", toggleLanguage);
  els.guideBtn.addEventListener("click", () => {
    window.location.href = "guide.html";
  });
  els.guideClose.addEventListener("click", closeGuide);
  els.guideOverlay.addEventListener("click", (event) => {
    if (event.target === els.guideOverlay) closeGuide();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeGuide();
  });

  document.querySelectorAll(".segmented button").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".segmented button").forEach((b) => b.classList.remove("active"));
      button.classList.add("active");
      state.riskFilter = button.dataset.risk;
      render();
    });
  });
}

function toggleLanguage() {
  state.lang = state.lang === "ko" ? "en" : "ko";
  document.documentElement.lang = state.lang;
  localStorage.setItem("obtExceptionLang", state.lang);
  applyLanguage();
  populateBaseFilters();
  refreshDependentFilters();
  render();
  if (!els.guideOverlay.classList.contains("hidden")) renderGuide();
}

function closeGuide() {
  els.guideOverlay.classList.add("hidden");
}

function t(path) {
  const parts = path.split(".");
  let value = I18N[state.lang] || I18N.ko;
  for (const part of parts) {
    value = value && value[part];
  }
  if (value != null) return value;
  value = I18N.ko;
  for (const part of parts) {
    value = value && value[part];
  }
  return value != null ? value : path;
}

function applyLanguage() {
  els.langToggle.dataset.lang = state.lang;
  els.langToggle.setAttribute("aria-label", state.lang === "ko" ? "Switch to English" : "한국어로 전환");
  els.langToggle.setAttribute("aria-pressed", state.lang === "en" ? "true" : "false");
  if (els.guideLangToggle) els.guideLangToggle.textContent = t("langToggle");
  els.guideBtn.textContent = t("guide");
  els.refreshBtn.textContent = t("refresh");
  document.querySelector("#loading p").textContent = t("loading");

  setText('label[for="horizonFilter"]', t("filters.horizon"));
  setText('label[for="priorityFilter"]', t("filters.priority"));
  setText('label[for="monthFilter"]', t("filters.month"));
  setText('label[for="weekFilter"]', t("filters.week"));
  setText('label[for="originFilter"]', t("filters.origin"));
  setText('label[for="polFilter"]', t("filters.pol"));
  setText('label[for="destFilter"]', t("filters.dest"));
  setText('label[for="dstFilter"]', t("filters.dst"));
  setText('label[for="salesFilter"]', t("filters.sales"));
  setText('label[for="compareFilter"]', t("filters.compare"));
  setText('label[for="searchInput"]', t("filters.search"));
  els.searchInput.placeholder = state.lang === "ko" ? "화주 / 코드 / 영업사원 / Port" : "Customer / Code / Sales / Port";

  setText(".workbench .panel-wide h2", t("panels.route"));
  setText(".workbench .panel:not(.panel-wide) h2", t("panels.sales"));
  setText(".detail-grid .panel-wide h2", t("panels.shipper"));
  setText(".detail-grid .panel:not(.panel-wide) h2", t("panels.issue"));

  document.querySelectorAll(".segmented button").forEach((button) => {
    button.textContent = button.dataset.risk === "all" ? t("all") : button.dataset.risk === "high" ? t("high") : t("watch");
    button.title = riskFilterHelp(button.dataset.risk);
  });

  const routeHeads = document.querySelectorAll(".route-table thead th");
  const routeHeadLabels = [t("headers.route"), "TEU", "BSA", t("headers.trend"), "3W Signal", t("headers.action"), t("headers.judgment")];
  const routeHeadTips = [
    state.lang === "ko" ? "Origin/POL에서 도착국가/도착포트까지의 구간입니다." : "Lane from origin/POL to destination country/port.",
    state.lang === "ko" ? "현재 선택 기간의 norm_lst 합계입니다." : "Sum of norm_lst for the selected current period.",
    state.lang === "ko" ? "선택 주차의 BSA TEU와 현재 TEU 대비 소석/예상 Gap입니다." : "BSA TEU, utilization, and projected gap for selected weeks.",
    state.lang === "ko" ? "W+1/W+2/W+3 리드타임 성숙도와 최근 일별 증가속도입니다." : "Lead-time maturity and recent daily pickup.",
    state.lang === "ko" ? "w3_fst 기반 3주전 선행 부킹 신호와 w3_norm_lst 기반 확보/Late입니다." : "3W advance-booking signal from w3_fst, secured/Late from w3_norm_lst.",
    state.lang === "ko" ? "회복/재확보/선행/방어/대체 물량 중 우선 조치 방향입니다." : "Primary action across recovery, win-back, advance booking, protection, or substitute volume.",
    state.lang === "ko" ? "현재 행을 문제로 판단한 대표 리스크입니다." : "Primary risk explaining why this route is flagged."
  ];
  routeHeads.forEach((head, index) => {
    head.textContent = routeHeadLabels[index];
    head.title = routeHeadTips[index];
  });

  const shipperHeads = document.querySelectorAll(".detail-table thead th");
  const shipperHeadLabels = ["Priority", t("headers.shipper"), t("headers.sales"), t("headers.route"), t("headers.currentTeu"), t("headers.baseTeu"), t("headers.delta"), "3W TEU", t("headers.action"), t("headers.judgment")];
  shipperHeads.forEach((head, index) => {
    head.textContent = shipperHeadLabels[index];
  });

  if (state.raw) updateMeta(state.loadedPath || "data.json");
}

function setText(selector, text) {
  const el = document.querySelector(selector);
  if (el) el.textContent = text;
}

function riskFilterHelp(type) {
  if (state.lang === "en") {
    if (type === "high") return "Show high-severity rows only.";
    if (type === "watch") return "Show medium/watch rows.";
    return "Show all flagged rows.";
  }
  if (type === "high") return "High 심각도 행만 표시합니다.";
  if (type === "watch") return "중간/관찰 수준 행을 표시합니다.";
  return "선택 조건의 모든 특이사항을 표시합니다.";
}

function renderGuide() {
  els.guideTitle.textContent = state.lang === "en" ? "OBT Exception Monitor Guide" : "OBT Exception Monitor 사용 가이드";
  els.guideSubtitle.textContent = state.lang === "en"
    ? "How to read lead-time trend, BSA gap, customer recovery, and sales-owner actions"
    : "리드타임 트렌드, BSA Gap, 화주 회복, 영업사원 조치를 읽는 방법";
  els.guideBody.innerHTML = state.lang === "en" ? guideHtmlEn() : guideHtmlKo();
}

function guideHtmlKo() {
  return `
    <section>
      <h3>1. 화면의 목적</h3>
      <p>이 화면은 OBT 적재 데이터를 기준으로 잘되는 구간보다 문제가 있거나 조치가 필요한 구간을 먼저 보여줍니다. 국가, 선적지, 도착국가, 도착포트, 영업사원 필터를 조합해 특정 구간의 위험과 담당자별 조치 우선순위를 확인합니다.</p>
    </section>
    <section>
      <h3>2. 비교 기준</h3>
      <ul>
        <li><strong>현재 TEU</strong>: <code>norm_lst</code> 합계이며, 값이 없으면 <code>fst</code>를 사용합니다.</li>
        <li><strong>기준 TEU</strong>: 전월/전주/최근 3개월 비교 기준입니다. 차주, 차차주, 3주뒤를 선택하면 기준도 같은 리드타임 상태로 보정합니다.</li>
        <li><strong>W+3</strong>: 전월 같은 주차의 <code>w3_norm_lst</code> 또는 <code>w3_fst</code>를 기준으로 비교합니다.</li>
        <li><strong>W+2/W+1</strong>: 현재 원천에 WOS-2/WOS-1 필드가 없으므로 최종 TEU를 그대로 쓰지 않고 리드타임 성숙도 보정값으로 비교합니다.</li>
      </ul>
    </section>
    <section>
      <h3>3. 주요 KPI</h3>
      <ul>
        <li><strong>총 TEU</strong>: 선택된 현재 기간의 전체 TEU입니다.</li>
        <li><strong>BSA 대비 TEU</strong>: 현재 TEU / BSA입니다. 낮을수록 선복 미소석 위험이 큽니다.</li>
        <li><strong>트렌드/속도 부족</strong>: 리드타임 트렌드보다 늦거나 최근 부킹속도가 필요한 속도보다 낮은 구간 수입니다.</li>
        <li><strong>3W Booking TEU</strong>: <code>w3_fst</code> 기반 3주전 선행 부킹량입니다.</li>
        <li><strong>감소/이탈 화주</strong>: 회복 또는 재확보가 필요한 화주 단위 후보입니다.</li>
      </ul>
    </section>
    <section>
      <h3>4. P1/P2/P3</h3>
      <ul>
        <li><strong>P1</strong>: 당일 확인이 필요한 즉시 대응 구간입니다. 리드타임 지연, 큰 BSA Gap, 핵심 화주 감소가 강하게 반영됩니다.</li>
        <li><strong>P2</strong>: 우선 확인 대상입니다. 아직 회복 시간이 있지만 영업 조치가 필요합니다.</li>
        <li><strong>P3</strong>: 관찰 대상입니다. 영향이 작거나 참고 신호입니다.</li>
      </ul>
    </section>
    <section>
      <h3>5. 구간별 특이사항</h3>
      <ul>
        <li><strong>TEU</strong>: 현재 TEU와 현재 Active 화주 수를 보여줍니다.</li>
        <li><strong>BSA</strong>: BSA TEU, 소석률, 남은 기간 예상 Gap입니다.</li>
        <li><strong>트렌드/속도</strong>: W+1/W+2/W+3별 포트 기준 성숙도와 최근 일별 부킹속도입니다.</li>
        <li><strong>3W Signal</strong>: <code>w3_fst</code> TEU, 3W 화주 수, <code>w3_norm_lst</code> 기반 확보율과 Late 의존도를 보여줍니다.</li>
        <li><strong>판단</strong>: 대표 리스크입니다. 마우스를 올리면 정의와 접근 방식이 표시됩니다.</li>
      </ul>
    </section>
    <section>
      <h3>6. 리스크 유형</h3>
      <p>세부 태그는 많지만 화면에서는 6개 상위 유형으로 묶습니다. 세부 원인은 칩으로 유지되어 추적할 수 있습니다.</p>
      <ul>
        <li><strong>리드타임/속도</strong>: 현재 부킹이 같은 시점 기대치보다 느린 상태입니다.</li>
        <li><strong>BSA/선복 Gap</strong>: 트렌드는 정상이어도 BSA 목표 자체가 높아 Gap이 남는 상태를 포함합니다.</li>
        <li><strong>화주 기반 감소</strong>: 이탈, 급감, 감소, Active 화주수 감소입니다.</li>
        <li><strong>3W 선행 부킹</strong>: 3주전 선행 부킹 약화입니다.</li>
        <li><strong>수익/품질</strong>: 고수익, 취소, Late 의존 리스크입니다.</li>
        <li><strong>집중/변동</strong>: 특정 화주 쏠림, 신규 대형, 급증입니다.</li>
      </ul>
    </section>
    <section>
      <h3>7. 영업사원별 조치</h3>
      <p>담당자별 카드는 상태를 빠르게 읽기 위한 영역입니다. 영향 TEU, 관리 Gap, 트렌드 지연 구간, 회복/재확보 화주 수, 대표 구간을 함께 봅니다.</p>
      <ul>
        <li><strong>즉시 대응</strong>: P1이 많거나 Gap/트렌드 위험이 커 당일 대응이 필요합니다.</li>
        <li><strong>우선 확인</strong>: 회복 가능성이 있으나 조치 순위가 높은 상태입니다.</li>
        <li><strong>화주 회복</strong>: 감소/이탈 화주 중심으로 회복 또는 재확보가 필요합니다.</li>
        <li><strong>관찰</strong>: 영향이 작거나 추세 확인 대상입니다.</li>
      </ul>
    </section>
    <section>
      <h3>8. 권장 사용 순서</h3>
      <ol>
        <li>대응 시점을 차주, 차차주, 3주뒤로 나눠 확인합니다.</li>
        <li>도착국가/도착포트 필터로 중요 구간을 좁힙니다.</li>
        <li>구간별 판단이 리드타임 문제인지, BSA 목표 Gap인지 구분합니다.</li>
        <li>영업사원별 카드에서 담당자와 조치 방향을 확인합니다.</li>
        <li>화주 단위 Action List에서 실제 접촉 화주를 확인합니다.</li>
      </ol>
    </section>
  `;
}

function guideHtmlEn() {
  return `
    <section>
      <h3>1. Purpose</h3>
      <p>This dashboard focuses on OBT route exceptions rather than healthy areas. Use origin, POL, destination country, destination port, and sales-owner filters to find where action is needed.</p>
    </section>
    <section>
      <h3>2. Comparison Logic</h3>
      <ul>
        <li><strong>Current TEU</strong>: sum of <code>norm_lst</code>; <code>fst</code> is used when <code>norm_lst</code> is missing.</li>
        <li><strong>Baseline TEU</strong>: previous month, previous week, or 3-month average. For W+1/W+2/W+3, the baseline is aligned to the same lead-time stage.</li>
        <li><strong>W+3</strong>: compared against prior-period <code>w3_norm_lst</code> or <code>w3_fst</code> for the same week slot.</li>
        <li><strong>W+2/W+1</strong>: WOS-2/WOS-1 source fields are not available, so final TEU is maturity-adjusted rather than used directly.</li>
      </ul>
    </section>
    <section>
      <h3>3. KPI Definitions</h3>
      <ul>
        <li><strong>Total TEU</strong>: current selected-period TEU.</li>
        <li><strong>TEU vs BSA</strong>: current TEU divided by BSA. Lower values indicate space-utilization risk.</li>
        <li><strong>Trend/Pace Risk</strong>: routes behind lead-time maturity or required daily pickup.</li>
        <li><strong>3W Booking TEU</strong>: advance-booked TEU from <code>w3_fst</code>.</li>
        <li><strong>Declining/Lost Customers</strong>: customer-level candidates for recovery or win-back.</li>
      </ul>
    </section>
    <section>
      <h3>4. P1/P2/P3</h3>
      <ul>
        <li><strong>P1</strong>: immediate action. Strong lead-time delay, large BSA gap, or key-customer decline.</li>
        <li><strong>P2</strong>: priority check. There is still time, but sales action is needed.</li>
        <li><strong>P3</strong>: watch. Lower-impact or reference signal.</li>
      </ul>
    </section>
    <section>
      <h3>5. Route Exceptions</h3>
      <ul>
        <li><strong>TEU</strong>: current TEU and active customer count.</li>
        <li><strong>BSA</strong>: BSA TEU, utilization, and projected remaining gap.</li>
        <li><strong>Trend / Pace</strong>: port-level lead-time maturity benchmark and recent daily pickup.</li>
        <li><strong>3W Signal</strong>: <code>w3_fst</code> TEU, 3W customer count, secured rate and Late dependency from <code>w3_norm_lst</code>.</li>
        <li><strong>Judgment</strong>: primary risk. Hover over a chip to see definition and approach.</li>
      </ul>
    </section>
    <section>
      <h3>6. Risk Groups</h3>
      <p>Detailed tags remain available, but the dashboard groups them into six executive risk groups.</p>
      <ul>
        <li><strong>Lead Time / Pace</strong>: current booking is behind expected maturity for the same timing.</li>
        <li><strong>BSA / Space Gap</strong>: includes cases where trend is normal but BSA target is still high.</li>
        <li><strong>Customer Base Loss</strong>: churn, sharp decline, decline, or fewer active customers.</li>
        <li><strong>3W Advance Booking</strong>: weakened three-week advance booking.</li>
        <li><strong>Profit / Quality</strong>: high-profit, cancellation, or Late dependency risk.</li>
        <li><strong>Concentration / Volatility</strong>: concentration, new large volume, or sharp increase.</li>
      </ul>
    </section>
    <section>
      <h3>7. Sales Owner Cards</h3>
      <p>Owner cards summarize the operational state: impact TEU, action gap, trend-risk route count, recovery/win-back customer count, and top route.</p>
      <ul>
        <li><strong>Immediate</strong>: same-day follow-up needed.</li>
        <li><strong>Priority Check</strong>: still recoverable, but high priority.</li>
        <li><strong>Customer Recovery</strong>: focus on declining or lost customers.</li>
        <li><strong>Watch</strong>: lower impact or monitoring signal.</li>
      </ul>
    </section>
    <section>
      <h3>8. Recommended Workflow</h3>
      <ol>
        <li>Review W+1, W+2, and W+3 separately.</li>
        <li>Narrow by destination country and destination port.</li>
        <li>Separate lead-time delay from BSA target gap.</li>
        <li>Use sales-owner cards to assign action ownership.</li>
        <li>Use the Customer Action List to identify who to contact.</li>
      </ol>
    </section>
  `;
}

async function loadData(force = false) {
  showLoading(true);
  try {
    let data = null;
    let loadedPath = "";
    for (const path of DATA_PATHS) {
      try {
        const url = force ? `${path}?t=${Date.now()}` : path;
        const response = await fetch(url);
        if (!response.ok) continue;
        data = await response.json();
        loadedPath = path;
        break;
      } catch (error) {
        // Try the next path.
      }
    }
    if (!data) throw new Error("dist/data.json 파일을 읽을 수 없습니다.");

    state.raw = data;
    state.loadedPath = loadedPath;
    state.rows = normalizeRows(data.shipper || []);
    state.bsaRows = normalizeBsaRows(data.bsa || []);
    state.history = await loadHistory(force);
    state.months = Array.from(new Set(state.rows.map((row) => row.month))).sort();

    const targetMonth = defaultTargetMonth(data.data_date);
    state.filters.month = state.months.includes(targetMonth) ? targetMonth : state.months[state.months.length - 1];
    state.filters.week = "ALL";

    populateBaseFilters();
    refreshDependentFilters();
    render();
    updateMeta(loadedPath);
  } catch (error) {
    els.dataMeta.textContent = error.message;
    renderError(error.message);
  } finally {
    showLoading(false);
  }
}

async function loadHistory(force = false) {
  for (const path of HISTORY_PATHS) {
    try {
      const url = force ? `${path}?t=${Date.now()}` : path;
      const response = await fetch(url);
      if (!response.ok) continue;
      return await response.json();
    } catch (error) {
      // Optional file; continue without pace history.
    }
  }
  return null;
}

function normalizeRows(rows) {
  return rows
    .filter((row) => row.team === "OBT")
    .map((row) => {
      const shipperCode = clean(row.BKG_SHPR_CST_NO);
      const shipperName = clean(row.BKG_SHPR_CST_ENM);
      const sales = clean(row.Salesman_POR) || "미지정";
      const origin = clean(row.origin);
      const pol = clean(row.ori_port);
      const dest = clean(row.dest);
      const dst = clean(row.dst_port);
      const month = clean(row.YYYYMM);
      const week = clean(row.week_start_date);
      const teu = toNumber(row.norm_lst ?? row.fst);
      const w3Teu = toNumber(row.w3_fst);
      const w3NormTeu = toNumber(row.w3_norm_lst);
      const w3CancelTeu = toNumber(row.w3_canc_fst);
      const w3HiTeu = toNumber(row.w3_hi_fst);
      const w3HiNormTeu = toNumber(row.w3_hi_norm_lst);
      const w3RouteHiTeu = toNumber(row.w3_route_hi_fst);
      const w3Cm1 = toNumber(row.w3_cm1_norm);
      const search = [shipperCode, shipperName, sales, origin, pol, dest, dst].join(" ").toLowerCase();
      return {
        origin,
        pol,
        dest,
        dst,
        month,
        week,
        weekTime: parseKoreanDate(week),
        shipperCode,
        shipperName,
        sales,
        grade: clean(row.grade),
        profitTag: clean(row["고수익태그"]),
        teu,
        w3Teu,
        w3NormTeu,
        w3CancelTeu,
        w3HiTeu,
        w3HiNormTeu,
        w3RouteHiTeu,
        w3Cm1,
        search,
        routeKey: [origin, pol, dest, dst].join("|"),
        shipperKey: [shipperCode || shipperName, sales, origin, pol, dest, dst].join("|")
      };
    })
    .filter((row) => row.month && (row.teu > 0 || row.w3Teu > 0 || row.w3NormTeu > 0 || row.w3CancelTeu > 0));
}

function normalizeBsaRows(rows) {
  return rows
    .filter((row) => row.team === "OBT")
    .map((row) => {
      const origin = clean(row.origin);
      const pol = clean(row.POR_PORT);
      const dest = clean(row.dest);
      const dst = clean(row.DLY_PORT);
      const month = clean(row.YYYYMM);
      const ww = clean(row.WW);
      const bsaTeu = toNumber(row.teu_bsa);
      return {
        origin,
        pol,
        dest,
        dst,
        month,
        ww,
        bsaTeu,
        routeKey: [origin, pol, dest, dst].join("|")
      };
    })
    .filter((row) => row.month && row.bsaTeu > 0);
}

function populateBaseFilters() {
  setOptions(els.monthFilter, state.months.map((m) => [m, formatMonth(m)]), state.filters.month);
  setOptions(els.horizonFilter, [["next3", horizonOptionLabel("next3")], ["w1", horizonOptionLabel("w1")], ["w2", horizonOptionLabel("w2")], ["w3", horizonOptionLabel("w3")], ["custom", t("options.custom")]], state.filters.horizon);
  setOptions(els.priorityFilter, [["important", t("options.important")], ["all", t("options.all")]], state.filters.priority);
  setOptions(els.compareFilter, [["prevMonth", t("options.prevMonth")], ["prevWeek", t("options.prevWeek")], ["avg3", t("options.avg3")]], state.filters.compare);
}

function horizonOptionLabel(mode) {
  const base = t(`options.${mode}`);
  if (!state.rows.length) return base;
  const offsetsByMode = {
    w1: [1],
    w2: [2],
    w3: [3],
    next3: [1, 2, 3]
  };
  const offsets = offsetsByMode[mode];
  if (!offsets) return base;
  const currentStart = getCurrentWeekStartDate();
  const weeks = offsets.map((offset) => formatKoreanDate(addDays(currentStart, offset * 7)));
  return `${base} · ${weeks.map(weekLabelWithWW).join(", ")}`;
}

function refreshDependentFilters() {
  const forcedPeriod = getForcedPeriod();
  const useCustomPeriod = state.filters.horizon === "custom";
  els.monthFilter.disabled = !useCustomPeriod;
  els.weekFilter.disabled = !useCustomPeriod;

  const monthRows = useCustomPeriod
    ? state.rows.filter((row) => row.month === state.filters.month)
    : state.rows.filter((row) => forcedPeriod.weekSet.has(row.week));
  const weeks = Array.from(new Set(monthRows.map((row) => row.week)))
    .filter(Boolean)
    .sort((a, b) => parseKoreanDate(a) - parseKoreanDate(b));

  setOptions(els.weekFilter, [["ALL", t("all")], ...weeks.map((week) => [week, shortWeek(week)])], state.filters.week);

  const filteredForChoices = state.rows.filter((row) => {
    if (useCustomPeriod) {
      if (row.month !== state.filters.month) return false;
      if (state.filters.week !== "ALL" && row.week !== state.filters.week) return false;
    } else if (!forcedPeriod.weekSet.has(row.week)) {
      return false;
    }
    return true;
  });

  const origins = uniqueSorted(filteredForChoices.map((row) => row.origin));
  setOptions(els.originFilter, [["ALL", t("all")], ...origins.map((v) => [v, v])], state.filters.origin);

  const polRows = filteredForChoices.filter((row) => state.filters.origin === "ALL" || row.origin === state.filters.origin);
  const pols = uniqueSorted(polRows.map((row) => row.pol));
  setOptions(els.polFilter, [["ALL", t("all")], ...pols.map((v) => [v, v])], state.filters.pol);

  const destRows = polRows.filter((row) => state.filters.pol === "ALL" || row.pol === state.filters.pol);
  const dests = uniqueSorted(destRows.map((row) => row.dest));
  setOptions(els.destFilter, [["ALL", t("all")], ...dests.map((v) => [v, v])], state.filters.dest);

  const dstRows = destRows.filter((row) => state.filters.dest === "ALL" || row.dest === state.filters.dest);
  const dsts = uniqueSorted(dstRows.map((row) => row.dst));
  setOptions(els.dstFilter, [["ALL", t("all")], ...dsts.map((v) => [v, v])], state.filters.dst);

  const salesRows = dstRows.filter((row) => state.filters.dst === "ALL" || row.dst === state.filters.dst);
  const sales = uniqueSorted(salesRows.map((row) => row.sales));
  setOptions(els.salesFilter, [["ALL", t("all")], ...sales.map((v) => [v, v])], state.filters.sales);

  state.filters.week = els.weekFilter.value;
  state.filters.origin = els.originFilter.value;
  state.filters.pol = els.polFilter.value;
  state.filters.dest = els.destFilter.value;
  state.filters.dst = els.dstFilter.value;
  state.filters.sales = els.salesFilter.value;
  state.filters.compare = els.compareFilter.value;
  state.filters.horizon = els.horizonFilter.value;
  state.filters.priority = els.priorityFilter.value;
}

function render() {
  if (!state.rows.length) return;

  const periods = getPeriods();
  const currentRows = filterRowsForPeriod(periods.current);
  const baselineRows = buildBaselineRows(periods);
  const currentBsaRows = filterBsaForPeriod(periods.current);
  const baselineBsaRows = filterBsaForPeriod(periods.baseline);
  const analysis = analyze(currentRows, baselineRows, currentBsaRows, baselineBsaRows, periods);

  renderKpis(analysis);
  renderRoutes(analysis);
  renderSales(analysis);
  renderShippers(analysis);
  renderIssues(analysis);
}

function getPeriods() {
  if (state.filters.horizon !== "custom") {
    const current = getForcedPeriod();
    const baseline = getForcedBaselinePeriod(current);
    return {
      current,
      baseline,
      prevMonth: "",
      prevWeek: "",
      label: current.label,
      baselineLabel: baseline.label
    };
  }

  const current = {
    mode: "custom",
    month: state.filters.month,
    week: state.filters.week,
    weeks: [],
    weekSet: new Set()
  };

  const monthIndex = state.months.indexOf(state.filters.month);
  const prevMonth = monthIndex > 0 ? state.months[monthIndex - 1] : "";
  const weekList = Array.from(new Set(state.rows
    .filter((row) => row.month === state.filters.month)
    .map((row) => row.week)))
    .sort((a, b) => parseKoreanDate(a) - parseKoreanDate(b));
  const weekIndex = weekList.indexOf(state.filters.week);
  const prevWeek = weekIndex > 0 ? weekList[weekIndex - 1] : "";

  return {
    current,
    baseline: {
      mode: "custom",
      month: prevMonth,
      week: "ALL",
      weeks: [],
      weekSet: new Set(),
      label: prevMonth ? formatMonth(prevMonth) : "비교 기준 없음"
    },
    prevMonth,
    prevWeek,
    label: periodLabel(current),
    baselineLabel: baselineLabel(prevMonth, prevWeek)
  };
}

function baselineLabel(prevMonth, prevWeek) {
  if (state.filters.compare === "prevWeek" && state.filters.week !== "ALL" && prevWeek) {
    return weekLabelWithWW(prevWeek);
  }
  if (state.filters.compare === "avg3") return state.lang === "en" ? "3-month average" : "최근 3개월 평균";
  return prevMonth ? formatMonth(prevMonth) : (state.lang === "en" ? "no baseline" : "비교 기준 없음");
}

function periodLabel(period) {
  return `${formatMonth(period.month)} ${period.week === "ALL" ? t("all") : weekLabelWithWW(period.week)}`;
}

function filterRowsForPeriod(period) {
  if (!period) return [];
  return state.rows.filter((row) => {
    if (period.weekSet && period.weekSet.size) {
      if (!period.weekSet.has(row.week)) return false;
    } else {
      if (row.month !== period.month) return false;
      if (period.week !== "ALL" && row.week !== period.week) return false;
    }
    if (state.filters.origin !== "ALL" && row.origin !== state.filters.origin) return false;
    if (state.filters.pol !== "ALL" && row.pol !== state.filters.pol) return false;
    if (state.filters.dest !== "ALL" && row.dest !== state.filters.dest) return false;
    if (state.filters.dst !== "ALL" && row.dst !== state.filters.dst) return false;
    if (state.filters.sales !== "ALL" && row.sales !== state.filters.sales) return false;
    if (state.filters.query && !row.search.includes(state.filters.query)) return false;
    return true;
  });
}

function buildBaselineRows(periods) {
  if (periods.baseline && periods.baseline.mode === "forced") {
    const rows = filterRowsForPeriod(periods.baseline);
    const aligned = alignBaselineRowsToLeadStage(rows, periods.baseline);
    return periods.baseline.scaleDivisor ? aligned.map((row) => scaleRow(row, periods.baseline.scaleDivisor)) : aligned;
  }

  if (state.filters.compare === "avg3") {
    const monthIndex = state.months.indexOf(state.filters.month);
    const months = state.months.slice(Math.max(0, monthIndex - 3), monthIndex);
    const rows = state.rows.filter((row) => months.includes(row.month));
    const filtered = filterByDimensions(rows);
    if (!months.length) return [];
    return filtered.map((row) => scaleRow(row, months.length));
  }

  if (state.filters.compare === "prevWeek" && state.filters.week !== "ALL" && periods.prevWeek) {
    return filterRowsForPeriod({ month: state.filters.month, week: periods.prevWeek });
  }

  if (!periods.prevMonth) return [];
  return filterRowsForPeriod({ month: periods.prevMonth, week: "ALL" });
}

function filterBsaForPeriod(period) {
  if (!period) return [];
  const wwSet = new Set((period.weeks || []).map((week) => weekToWW(week)).filter(Boolean));
  const monthSet = new Set(period.months || []);
  const rows = state.bsaRows.filter((row) => {
    if (wwSet.size) {
      if (!wwSet.has(row.ww)) return false;
    } else if (period.month) {
      if (row.month !== period.month) return false;
      if (period.week && period.week !== "ALL") {
        const ww = weekToWW(period.week);
        if (ww && row.ww !== ww) return false;
      }
    } else if (monthSet.size && !monthSet.has(row.month)) {
      return false;
    }
    if (state.filters.origin !== "ALL" && row.origin !== state.filters.origin) return false;
    if (state.filters.pol !== "ALL" && row.pol !== state.filters.pol) return false;
    if (state.filters.dest !== "ALL" && row.dest !== state.filters.dest) return false;
    if (state.filters.dst !== "ALL" && row.dst !== state.filters.dst) return false;
    return true;
  });
  return period.scaleDivisor ? rows.map((row) => ({ ...row, bsaTeu: row.bsaTeu / period.scaleDivisor })) : rows;
}

function getForcedPeriod() {
  const offsetsByMode = {
    w1: [1],
    w2: [2],
    w3: [3],
    next3: [1, 2, 3]
  };
  const offsets = offsetsByMode[state.filters.horizon] || offsetsByMode.next3;
  const currentStart = getCurrentWeekStartDate();
  const weeks = offsets.map((offset) => formatKoreanDate(addDays(currentStart, offset * 7)));
  const months = uniqueSorted(weeks.map((week) => weekToMonth(week)).filter(Boolean));
  return {
    mode: "forced",
    horizon: state.filters.horizon,
    offsets,
    weeks,
    weekSet: new Set(weeks),
    months,
    label: horizonLabel(state.filters.horizon, weeks)
  };
}

function getForcedBaselinePeriod(current) {
  if (state.filters.compare === "prevWeek") {
    const leadOffsetByWeek = {};
    const weeks = current.weeks.map((week, index) => {
      const baselineWeek = formatKoreanDate(addDays(koreanWeekToDate(week), -7));
      leadOffsetByWeek[baselineWeek] = current.offsets ? current.offsets[index] : weekLeadOffset(week, getCurrentWeekStartDate());
      return baselineWeek;
    });
    return {
      mode: "forced",
      weeks,
      weekSet: new Set(weeks),
      months: uniqueSorted(weeks.map((week) => weekToMonth(week)).filter(Boolean)),
      leadOffsetByWeek,
      leadAligned: true,
      label: state.lang === "en" ? "previous week" : "직전 주차"
    };
  }

  const monthBack = state.filters.compare === "avg3" ? 3 : 1;
  const weeks = [];
  const leadOffsetByWeek = {};
  current.weeks.forEach((week, index) => {
    const baselineWeeks = previousMonthSameSlotWeeks(week, monthBack);
    baselineWeeks.forEach((baselineWeek) => {
      leadOffsetByWeek[baselineWeek] = current.offsets ? current.offsets[index] : weekLeadOffset(week, getCurrentWeekStartDate());
      weeks.push(baselineWeek);
    });
  });
  const uniqWeeks = Array.from(new Set(weeks)).filter(Boolean);
  return {
    mode: "forced",
    weeks: uniqWeeks,
    weekSet: new Set(uniqWeeks),
    months: uniqueSorted(uniqWeeks.map((week) => weekToMonth(week)).filter(Boolean)),
    scaleDivisor: state.filters.compare === "avg3" ? 3 : 0,
    leadOffsetByWeek,
    leadAligned: true,
    label: state.filters.compare === "avg3"
      ? (state.lang === "en" ? "3-month same-week average" : "최근 3개월 동일 주차 평균")
      : (state.lang === "en" ? "previous-month same week" : "전월 동일 주차")
  };
}

function alignBaselineRowsToLeadStage(rows, period) {
  if (!period || !period.leadOffsetByWeek) return rows;
  return rows.map((row) => {
    const offset = period.leadOffsetByWeek[row.week];
    if (!offset) return row;
    const teu = leadComparableTeu(row, offset);
    return {
      ...row,
      teu,
      leadAlignedTeu: teu,
      leadOffset: offset
    };
  });
}

function leadComparableTeu(row, offset) {
  const finalTeu = row.teu || 0;
  const w3Teu = row.w3NormTeu || row.w3Teu || 0;
  if (offset >= 3) return w3Teu;
  if (offset === 2) {
    if (finalTeu && w3Teu) return Math.min(finalTeu, Math.max(w3Teu, w3Teu + (finalTeu - w3Teu) * .45));
    return finalTeu * leadDefaultRate(2);
  }
  if (offset <= 1) {
    if (finalTeu && w3Teu) return Math.min(finalTeu, Math.max(w3Teu, w3Teu + (finalTeu - w3Teu) * .75));
    return finalTeu * leadDefaultRate(1);
  }
  return finalTeu;
}

function previousMonthSameSlotWeeks(week, monthBack) {
  const month = weekToMonth(week);
  if (!month) return [];
  const sourceWeeks = weeksForMonth(month);
  const slot = sourceWeeks.indexOf(week);
  if (slot < 0) return [];

  const weeks = [];
  for (let i = 1; i <= monthBack; i += 1) {
    const targetMonth = shiftMonth(month, -i);
    const targetWeeks = weeksForMonth(targetMonth);
    if (targetWeeks[slot]) weeks.push(targetWeeks[slot]);
  }
  return weeks;
}

function weeksForMonth(month) {
  return Array.from(new Set(state.rows
    .filter((row) => row.month === month)
    .map((row) => row.week)))
    .filter(Boolean)
    .sort((a, b) => parseKoreanDate(a) - parseKoreanDate(b));
}

function filterByDimensions(rows) {
  return rows.filter((row) => {
    if (state.filters.origin !== "ALL" && row.origin !== state.filters.origin) return false;
    if (state.filters.pol !== "ALL" && row.pol !== state.filters.pol) return false;
    if (state.filters.dest !== "ALL" && row.dest !== state.filters.dest) return false;
    if (state.filters.dst !== "ALL" && row.dst !== state.filters.dst) return false;
    if (state.filters.sales !== "ALL" && row.sales !== state.filters.sales) return false;
    if (state.filters.query && !row.search.includes(state.filters.query)) return false;
    return true;
  });
}

function analyze(currentRows, baselineRows, currentBsaRows, baselineBsaRows, periods) {
  const currentShippers = aggregateByShipper(currentRows);
  const baselineShippers = aggregateByShipper(baselineRows);
  const currentRoutes = aggregateByRoute(currentRows);
  const baselineRoutes = aggregateByRoute(baselineRows);
  const currentBsaRoutes = aggregateBsaByRoute(currentBsaRows);
  const baselineBsaRoutes = aggregateBsaByRoute(baselineBsaRows);
  const routeContext = buildRouteContext(currentRoutes, baselineRoutes, currentBsaRoutes, baselineBsaRoutes);
  const paceMap = buildPaceMap(periods, routeContext);
  mergePaceIntoContext(routeContext, paceMap);
  const leadTrendMap = buildLeadTrendMap(periods, currentRows, baselineRows, currentBsaRows, routeContext);
  mergeLeadTrendIntoContext(routeContext, leadTrendMap);

  const allShipperExceptions = buildShipperExceptions(currentShippers, baselineShippers, routeContext);
  const allRouteExceptions = buildRouteExceptions(currentRoutes, baselineRoutes, allShipperExceptions, routeContext, currentBsaRoutes, baselineBsaRoutes);
  const shipperExceptions = applyPriorityFilter(allShipperExceptions);
  const routeExceptions = applyPriorityFilter(allRouteExceptions);
  const salesActions = buildSalesActions(shipperExceptions, routeExceptions, currentRows, baselineRows);
  const issueSummary = summarizeIssues(shipperExceptions, routeExceptions);

  const totalCurrentTeu = sumMap(currentRoutes, "teu");
  const totalBaseTeu = sumMap(baselineRoutes, "teu");
  const totalBsaTeu = sumMap(currentBsaRoutes, "bsaTeu");
  const totalCurrentW3Teu = sumMap(currentRoutes, "w3Teu");
  const totalBaseW3Teu = sumMap(baselineRoutes, "w3Teu");
  const totalCurrentW3NormTeu = sumMap(currentRoutes, "w3NormTeu");
  const totalBaseW3NormTeu = sumMap(baselineRoutes, "w3NormTeu");
  const activeCurrent = distinctCountWhen(currentRows, shipperId, (row) => row.teu > 0);
  const activeBase = distinctCountWhen(baselineRows, shipperId, (row) => row.teu > 0);
  const w3ActiveCurrent = distinctCountWhen(currentRows, shipperId, (row) => row.w3Teu > 0);
  const w3ActiveBase = distinctCountWhen(baselineRows, shipperId, (row) => row.w3Teu > 0);
  const issueShippers = shipperExceptions.filter((row) => row.level !== "watch");
  const impactTeu = issueShippers.reduce((sum, row) => sum + row.impactTeu, 0);
  const bsaImpactTeu = issueShippers
    .filter((row) => row.bsaTeu > 0)
    .reduce((sum, row) => sum + row.impactTeu, 0);
  const speedRiskRoutes = routeExceptions.filter((row) => ["stalled", "slow", "short"].includes(row.paceStatus) || ["trend-short", "trend-slow"].includes(row.leadTrendStatus));
  const projectedGapTeu = routeExceptions.reduce((sum, row) => sum + (row.projectedGap || 0), 0);
  const p1Actions = shipperExceptions.filter((row) => row.priority === "P1").length + routeExceptions.filter((row) => row.priority === "P1").length;

  return {
    periods,
    currentRows,
    baselineRows,
    currentRoutes,
    baselineRoutes,
    currentBsaRoutes,
    baselineBsaRoutes,
    routeContext,
    paceMap,
    leadTrendMap,
    allRouteExceptions,
    allShipperExceptions,
    routeExceptions,
    shipperExceptions,
    salesActions,
    issueSummary,
    totals: {
      totalCurrentTeu,
      totalBaseTeu,
      totalBsaTeu,
      deltaTeu: totalCurrentTeu - totalBaseTeu,
      totalCurrentW3Teu,
      totalBaseW3Teu,
      deltaW3Teu: totalCurrentW3Teu - totalBaseW3Teu,
      totalCurrentW3NormTeu,
      totalBaseW3NormTeu,
      w3SecuredRate: totalCurrentTeu ? totalCurrentW3NormTeu / totalCurrentTeu : 0,
      baseW3SecuredRate: totalBaseTeu ? totalBaseW3NormTeu / totalBaseTeu : 0,
      activeCurrent,
      activeBase,
      activeDelta: activeCurrent - activeBase,
      w3ActiveCurrent,
      w3ActiveBase,
      w3ActiveDelta: w3ActiveCurrent - w3ActiveBase,
      issueShippers: issueShippers.length,
      impactTeu,
      bsaImpactTeu,
      speedRiskRoutes: speedRiskRoutes.length,
      projectedGapTeu,
      p1Actions,
      topActionCount: Math.min(60, shipperExceptions.length),
      actionSales: salesActions.length,
      highRoutes: routeExceptions.filter((row) => row.level === "high").length,
      importantRoutes: Array.from(routeContext.values()).filter((row) => row.isImportant).length,
      bsaRoutes: Array.from(routeContext.values()).filter((row) => row.bsaTeu > 0).length,
      allIssueShippers: allShipperExceptions.filter((row) => row.level !== "watch").length
    }
  };
}

function aggregateByShipper(rows) {
  const map = new Map();
  rows.forEach((row) => {
    const found = map.get(row.shipperKey) || {
      key: row.shipperKey,
      shipperCode: row.shipperCode,
      shipperName: row.shipperName || row.shipperCode || "미지정",
      sales: row.sales,
      origin: row.origin,
      pol: row.pol,
      dest: row.dest,
      dst: row.dst,
      routeKey: row.routeKey,
      teu: 0,
      w3Teu: 0,
      w3NormTeu: 0,
      w3CancelTeu: 0,
      w3HiTeu: 0,
      w3HiNormTeu: 0,
      w3RouteHiTeu: 0,
      w3Cm1: 0,
      weeks: new Set()
    };
    found.teu += row.teu;
    found.w3Teu += row.w3Teu;
    found.w3NormTeu += row.w3NormTeu;
    found.w3CancelTeu += row.w3CancelTeu;
    found.w3HiTeu += row.w3HiTeu;
    found.w3HiNormTeu += row.w3HiNormTeu;
    found.w3RouteHiTeu += row.w3RouteHiTeu;
    found.w3Cm1 += row.w3Cm1;
    found.weeks.add(row.week);
    map.set(row.shipperKey, found);
  });
  return map;
}

function aggregateByRoute(rows) {
  const map = new Map();
  rows.forEach((row) => {
    const found = map.get(row.routeKey) || {
      key: row.routeKey,
      origin: row.origin,
      pol: row.pol,
      dest: row.dest,
      dst: row.dst,
      teu: 0,
      w3Teu: 0,
      w3NormTeu: 0,
      w3CancelTeu: 0,
      w3HiTeu: 0,
      w3HiNormTeu: 0,
      w3RouteHiTeu: 0,
      w3Cm1: 0,
      shippers: new Set(),
      w3Shippers: new Set(),
      shipperTeu: new Map()
    };
    found.teu += row.teu;
    const shipperKey = row.shipperCode || row.shipperName;
    if (row.teu > 0) {
      found.shippers.add(shipperKey);
      found.shipperTeu.set(shipperKey, (found.shipperTeu.get(shipperKey) || 0) + row.teu);
    }
    if (row.w3Teu > 0) {
      found.w3Shippers.add(shipperKey);
    }
    found.w3Teu += row.w3Teu;
    found.w3NormTeu += row.w3NormTeu;
    found.w3CancelTeu += row.w3CancelTeu;
    found.w3HiTeu += row.w3HiTeu;
    found.w3HiNormTeu += row.w3HiNormTeu;
    found.w3RouteHiTeu += row.w3RouteHiTeu;
    found.w3Cm1 += row.w3Cm1;
    map.set(row.routeKey, found);
  });
  return map;
}

function aggregateByRouteWeek(rows) {
  const map = new Map();
  rows.forEach((row) => {
    const key = routeWeekKey(row.routeKey, row.week);
    const found = map.get(key) || {
      key,
      routeKey: row.routeKey,
      week: row.week,
      origin: row.origin,
      pol: row.pol,
      dest: row.dest,
      dst: row.dst,
      teu: 0,
      w3Teu: 0,
      shippers: new Set(),
      w3Shippers: new Set()
    };
    found.teu += row.teu;
    found.w3Teu += row.w3Teu;
    const shipperKey = row.shipperCode || row.shipperName;
    if (shipperKey && row.teu > 0) found.shippers.add(shipperKey);
    if (shipperKey && row.w3Teu > 0) found.w3Shippers.add(shipperKey);
    map.set(key, found);
  });
  return map;
}

function aggregateBsaByRouteWeek(rows, weeks) {
  const wwToWeek = new Map((weeks || []).map((week) => [weekToWW(week), week]).filter(([ww]) => ww));
  const map = new Map();
  rows.forEach((row) => {
    const week = wwToWeek.get(row.ww);
    if (!week) return;
    const key = routeWeekKey(row.routeKey, week);
    const found = map.get(key) || {
      key,
      routeKey: row.routeKey,
      week,
      origin: row.origin,
      pol: row.pol,
      dest: row.dest,
      dst: row.dst,
      bsaTeu: 0
    };
    found.bsaTeu += row.bsaTeu;
    map.set(key, found);
  });
  return map;
}

function routeWeekKey(routeKey, week) {
  return `${routeKey}@@${week}`;
}

function aggregateBsaByRoute(rows) {
  const map = new Map();
  rows.forEach((row) => {
    const found = map.get(row.routeKey) || {
      key: row.routeKey,
      origin: row.origin,
      pol: row.pol,
      dest: row.dest,
      dst: row.dst,
      bsaTeu: 0
    };
    found.bsaTeu += row.bsaTeu;
    map.set(row.routeKey, found);
  });
  return map;
}

function buildRouteContext(currentRoutes, baselineRoutes, currentBsaRoutes, baselineBsaRoutes) {
  const keys = new Set([...currentRoutes.keys(), ...baselineRoutes.keys(), ...currentBsaRoutes.keys(), ...baselineBsaRoutes.keys()]);
  const portThresholds = buildPortThresholds(keys, currentRoutes, baselineRoutes, currentBsaRoutes, baselineBsaRoutes);
  const context = new Map();
  keys.forEach((key) => {
    const current = currentRoutes.get(key);
    const base = baselineRoutes.get(key);
    const bsa = currentBsaRoutes.get(key);
    const baseBsa = baselineBsaRoutes.get(key);
    const template = current || base || bsa || baseBsa;
    const currentTeu = current ? current.teu : 0;
    const baseTeu = base ? base.teu : 0;
    const currentW3Teu = current ? current.w3Teu : 0;
    const baseW3Teu = base ? base.w3Teu : 0;
    const bsaTeu = bsa ? bsa.bsaTeu : 0;
    const portRule = portThresholds.get(template.dst) || { volume: 50, bsa: 100, impact: 30 };
    const maxVolume = Math.max(currentTeu, baseTeu, currentW3Teu, baseW3Teu);
    const impactSeed = Math.max(0, baseTeu - currentTeu, baseW3Teu - currentW3Teu, bsaTeu - currentTeu);
    const isImportant = bsaTeu >= portRule.bsa || maxVolume >= portRule.volume || impactSeed >= portRule.impact;
    context.set(key, {
      key,
      origin: template.origin,
      pol: template.pol,
      dest: template.dest,
      dst: template.dst,
      currentTeu,
      baseTeu,
      currentW3Teu,
      baseW3Teu,
      bsaTeu,
      maxVolume,
      portVolumeThreshold: portRule.volume,
      portBsaThreshold: portRule.bsa,
      portImpactThreshold: portRule.impact,
      baseBsaTeu: baseBsa ? baseBsa.bsaTeu : 0,
      isImportant
    });
  });
  return context;
}

function buildPortThresholds(keys, currentRoutes, baselineRoutes, currentBsaRoutes, baselineBsaRoutes) {
  const byPort = new Map();
  keys.forEach((key) => {
    const current = currentRoutes.get(key);
    const base = baselineRoutes.get(key);
    const bsa = currentBsaRoutes.get(key);
    const baseBsa = baselineBsaRoutes.get(key);
    const template = current || base || bsa || baseBsa;
    if (!template || !template.dst) return;
    const found = byPort.get(template.dst) || { volumes: [], bsas: [], impacts: [] };
    const currentTeu = current ? current.teu : 0;
    const baseTeu = base ? base.teu : 0;
    const currentW3Teu = current ? current.w3Teu : 0;
    const baseW3Teu = base ? base.w3Teu : 0;
    const bsaTeu = bsa ? bsa.bsaTeu : 0;
    const maxVolume = Math.max(currentTeu, baseTeu, currentW3Teu, baseW3Teu);
    const impactSeed = Math.max(0, baseTeu - currentTeu, baseW3Teu - currentW3Teu, bsaTeu - currentTeu);
    if (maxVolume > 0) found.volumes.push(maxVolume);
    if (bsaTeu > 0) found.bsas.push(bsaTeu);
    if (impactSeed > 0) found.impacts.push(impactSeed);
    byPort.set(template.dst, found);
  });

  const thresholds = new Map();
  byPort.forEach((values, dst) => {
    const volume = Math.max(50, quantile(values.volumes, .75));
    const bsa = Math.max(100, quantile(values.bsas, .75));
    const impact = Math.max(30, quantile(values.impacts, .70), volume * .35);
    thresholds.set(dst, { volume, bsa, impact });
  });
  return thresholds;
}

function buildPaceMap(periods, routeContext) {
  const snapshots = (state.history && state.history.snapshots || []).slice().sort((a, b) => String(a.data_date).localeCompare(String(b.data_date)));
  if (snapshots.length < 2) return new Map();

  const latestDate = String(state.raw && state.raw.data_date || snapshots[snapshots.length - 1].data_date);
  const latest = snapshots.find((snapshot) => String(snapshot.data_date) === latestDate) || snapshots[snapshots.length - 1];
  const currentDate = parseDataDate(latest.data_date);
  const selectedWeeks = periodWeeks(periods.current);
  const latestAgg = aggregateHistoryRoutes(latest, selectedWeeks);
  const prior3 = pickPriorSnapshot(snapshots, latest.data_date, 3);
  const prior7 = pickPriorSnapshot(snapshots, latest.data_date, 7);
  const prior3Agg = prior3 ? aggregateHistoryRoutes(prior3, selectedWeeks) : new Map();
  const prior7Agg = prior7 ? aggregateHistoryRoutes(prior7, selectedWeeks) : new Map();
  const daysRemaining = daysUntilPeriodEnd(periods.current, currentDate);
  const paceMap = new Map();

  routeContext.forEach((context, routeKey) => {
    const latestRow = latestAgg.get(routeKey) || {};
    const p3 = prior3Agg.get(routeKey) || {};
    const p7 = prior7Agg.get(routeKey) || {};
    const days3 = prior3 ? Math.max(1, diffDays(parseDataDate(prior3.data_date), currentDate)) : 0;
    const days7 = prior7 ? Math.max(1, diffDays(parseDataDate(prior7.data_date), currentDate)) : 0;
    const pace3 = days3 ? ((latestRow.teu || 0) - (p3.teu || 0)) / days3 : null;
    const pace7 = days7 ? ((latestRow.teu || 0) - (p7.teu || 0)) / days7 : null;
    const w3Pace3 = days3 ? ((latestRow.w3Teu || 0) - (p3.w3Teu || 0)) / days3 : null;
    const gap = Math.max(0, (context.bsaTeu || 0) - (context.currentTeu || 0));
    const requiredDaily = gap > 0 ? gap / Math.max(1, daysRemaining) : 0;
    const usablePace = Math.max(0, pace3 == null ? 0 : pace3);
    const projectedTeu = (context.currentTeu || 0) + usablePace * Math.max(1, daysRemaining);
    const projectedGap = Math.max(0, (context.bsaTeu || 0) - projectedTeu);
    const paceRatio = requiredDaily ? usablePace / requiredDaily : null;
    const status = paceStatus({ gap, requiredDaily, pace3: usablePace, paceRatio, projectedGap, bsaTeu: context.bsaTeu || 0 });

    paceMap.set(routeKey, {
      routeKey,
      daysRemaining,
      pace3,
      pace7,
      w3Pace3,
      requiredDaily,
      paceRatio,
      projectedTeu,
      projectedGap,
      status,
      latestTeu: latestRow.teu || 0,
      latestW3Teu: latestRow.w3Teu || 0,
      historyPoints: snapshots.length
    });
  });

  return paceMap;
}

function aggregateHistoryRoutes(snapshot, selectedWeeks) {
  const weekSet = new Set(selectedWeeks || []);
  const map = new Map();
  (snapshot.routes || []).forEach((item) => {
    const routeKey = item[0];
    const week = item[1];
    if (weekSet.size && !weekSet.has(week)) return;
    const found = map.get(routeKey) || { teu: 0, w3Teu: 0, active: 0, w3Active: 0 };
    found.teu += Number(item[2] || 0);
    found.w3Teu += Number(item[3] || 0);
    found.active += Number(item[4] || 0);
    found.w3Active += Number(item[5] || 0);
    map.set(routeKey, found);
  });
  return map;
}

function mergePaceIntoContext(routeContext, paceMap) {
  routeContext.forEach((context, routeKey) => {
    const pace = paceMap.get(routeKey);
    if (!pace) return;
    Object.assign(context, pace);
    const speedImportant = ["stalled", "slow", "short"].includes(pace.status) && (context.bsaTeu || 0) >= (context.portBsaThreshold || 100);
    const projectedImportant = (pace.projectedGap || 0) >= Math.max(50, (context.portImpactThreshold || 30), (context.bsaTeu || 0) * .12);
    if (speedImportant || projectedImportant) {
      context.isImportant = true;
    }
  });
}

function buildLeadTrendMap(periods, currentRows, baselineRows, currentBsaRows, routeContext) {
  const selectedWeeks = periodWeeks(periods.current);
  if (!selectedWeeks.length) return new Map();

  const currentByWeek = aggregateByRouteWeek(currentRows);
  const bsaByWeek = aggregateBsaByRouteWeek(currentBsaRows, selectedWeeks);
  const baselineWeeks = aggregateBaselineByCurrentWeek(periods, baselineRows);
  const currentDate = parseDataDate(state.raw && state.raw.data_date) || new Date();
  const records = [];

  routeContext.forEach((context, routeKey) => {
    selectedWeeks.forEach((week) => {
      const key = routeWeekKey(routeKey, week);
      const current = currentByWeek.get(key);
      const bsa = bsaByWeek.get(key);
      const baseTeu = baselineWeeks.get(key) || 0;
      const fallbackBase = context.baseTeu && selectedWeeks.length ? context.baseTeu / selectedWeeks.length : 0;
      const fallbackBsa = context.bsaTeu && selectedWeeks.length ? context.bsaTeu / selectedWeeks.length : 0;
      const currentTeu = current ? current.teu : 0;
      const bsaTeu = bsa ? bsa.bsaTeu : fallbackBsa;
      const targetTeu = Math.max(bsaTeu || 0, baseTeu || fallbackBase || 0, currentTeu || 0);
      if (targetTeu <= 0) return;
      const leadOffset = weekLeadOffset(week, currentDate);
      if (leadOffset < 0) return;
      records.push({
        routeKey,
        week,
        leadOffset,
        leadLabel: leadOffset ? `W+${leadOffset}` : "금주",
        dst: context.dst,
        currentTeu,
        targetTeu,
        bsaTeu,
        baseTeu: baseTeu || fallbackBase || 0,
        completion: currentTeu / targetTeu
      });
    });
  });

  const benchmarks = buildLeadTrendBenchmarks(records);
  const map = new Map();
  records.forEach((record) => {
    const benchmark = pickLeadTrendBenchmark(benchmarks, record);
    const expectedRate = benchmark ? benchmark.rate : leadDefaultRate(record.leadOffset);
    const expectedTeu = expectedRate * record.targetTeu;
    const gap = Math.max(0, expectedTeu - record.currentTeu);
    const impactThreshold = (routeContext.get(record.routeKey) || {}).portImpactThreshold || 30;
    const status = leadTrendStatus(record, expectedRate, gap, impactThreshold);
    const found = map.get(record.routeKey) || {
      routeKey: record.routeKey,
      currentTeu: 0,
      targetTeu: 0,
      expectedTeu: 0,
      trendGap: 0,
      weeks: [],
      worst: null,
      status: "trend-ok"
    };
    found.currentTeu += record.currentTeu;
    found.targetTeu += record.targetTeu;
    found.expectedTeu += expectedTeu;
    found.trendGap += gap;
    const enriched = {
      ...record,
      expectedRate,
      expectedTeu,
      gap,
      status,
      benchmarkCount: benchmark ? benchmark.count : 0
    };
    found.weeks.push(enriched);
    if (!found.worst || leadTrendSeverity(enriched) > leadTrendSeverity(found.worst)) {
      found.worst = enriched;
    }
    map.set(record.routeKey, found);
  });

  map.forEach((row) => {
    const statuses = row.weeks.map((week) => week.status);
    row.status = statuses.includes("trend-short") ? "trend-short" : statuses.includes("trend-slow") ? "trend-slow" : "trend-ok";
    row.currentRate = row.targetTeu ? row.currentTeu / row.targetTeu : 0;
    row.expectedRate = row.targetTeu ? row.expectedTeu / row.targetTeu : 0;
    row.label = leadTrendLabel(row);
  });

  return map;
}

function aggregateBaselineByCurrentWeek(periods, baselineRows) {
  const currentWeeks = periodWeeks(periods.current);
  const baselineWeeks = periodWeeks(periods.baseline);
  const mappedRows = [];
  const mapWeek = new Map();

  if (currentWeeks.length && baselineWeeks.length) {
    if (baselineWeeks.length === currentWeeks.length) {
      baselineWeeks.forEach((week, index) => mapWeek.set(week, currentWeeks[index]));
    } else if (baselineWeeks.length % currentWeeks.length === 0) {
      const groupSize = baselineWeeks.length / currentWeeks.length;
      baselineWeeks.forEach((week, index) => mapWeek.set(week, currentWeeks[Math.floor(index / groupSize)]));
    }
  }

  baselineRows.forEach((row) => {
    const week = mapWeek.get(row.week);
    if (!week) return;
    mappedRows.push({ ...row, week });
  });

  const byWeek = aggregateByRouteWeek(mappedRows);
  const result = new Map();
  byWeek.forEach((row, key) => {
    result.set(key, row.teu || 0);
  });
  return result;
}

function buildLeadTrendBenchmarks(records) {
  const groups = new Map();
  const add = (key, value) => {
    const found = groups.get(key) || [];
    found.push(value);
    groups.set(key, found);
  };

  records.forEach((record) => {
    if (record.targetTeu < 10) return;
    const capped = Math.max(0, Math.min(1.2, record.completion));
    add(`${record.dst}|${record.leadOffset}`, capped);
    add(`ALL|${record.leadOffset}`, capped);
  });

  const benchmarks = new Map();
  groups.forEach((values, key) => {
    const fallback = leadDefaultRate(key.split("|").at(-1));
    const rateValue = values.length ? quantileAll(values, .35) : fallback;
    const medianValue = values.length ? quantileAll(values, .5) : fallback;
    benchmarks.set(key, {
      rate: Math.max(.05, Math.min(.98, rateValue)),
      median: Math.max(.05, Math.min(.98, medianValue)),
      count: values.length
    });
  });
  return benchmarks;
}

function pickLeadTrendBenchmark(benchmarks, record) {
  const port = benchmarks.get(`${record.dst}|${record.leadOffset}`);
  if (port && port.count >= 5) return port;
  return benchmarks.get(`ALL|${record.leadOffset}`) || port || null;
}

function leadTrendStatus(record, expectedRate, gap, impactThreshold) {
  const gapRatio = expectedRate ? (expectedRate - record.completion) / expectedRate : 0;
  if (gap >= Math.max(impactThreshold, record.targetTeu * .12) && gapRatio >= .25) return "trend-short";
  if (gap >= Math.max(10, impactThreshold * .45) && gapRatio >= .12) return "trend-slow";
  return "trend-ok";
}

function leadTrendSeverity(row) {
  if (!row) return 0;
  const statusScore = row.status === "trend-short" ? 3 : row.status === "trend-slow" ? 2 : 1;
  return statusScore * 100000 + (row.gap || 0);
}

function mergeLeadTrendIntoContext(routeContext, leadTrendMap) {
  routeContext.forEach((context, routeKey) => {
    const trend = leadTrendMap.get(routeKey);
    if (!trend) return;
    Object.assign(context, {
      leadTrendStatus: trend.status,
      leadTrendGap: trend.trendGap,
      leadTrendExpectedTeu: trend.expectedTeu,
      leadTrendCurrentRate: trend.currentRate,
      leadTrendExpectedRate: trend.expectedRate,
      leadTrendLabel: trend.label,
      leadTrendWorst: trend.worst,
      leadTrendWeeks: trend.weeks
    });
    if (["trend-short", "trend-slow"].includes(trend.status) && trend.trendGap >= Math.max(30, context.portImpactThreshold || 30)) {
      context.isImportant = true;
    }
  });
}

function leadTrendLabel(trend) {
  if (!trend || !trend.worst) return "트렌드 이력 없음";
  const worst = trend.worst;
  return `${worst.leadLabel} ${rpct(worst.completion)} vs ${rpct(worst.expectedRate)} · Gap ${fmt(worst.gap)}`;
}

function weekLeadOffset(week, currentDate) {
  const weekDate = koreanWeekToDate(week);
  return Math.round(diffDays(currentDate, weekDate) / 7);
}

function leadDefaultRate(leadOffset) {
  const lead = Number(leadOffset);
  if (lead <= 1) return .72;
  if (lead === 2) return .52;
  if (lead === 3) return .34;
  return .25;
}

function pickPriorSnapshot(snapshots, latestDate, targetDays) {
  const latest = parseDataDate(latestDate);
  let best = null;
  let bestDelta = Infinity;
  snapshots.forEach((snapshot) => {
    if (String(snapshot.data_date) >= String(latestDate)) return;
    const delta = diffDays(parseDataDate(snapshot.data_date), latest);
    if (delta >= targetDays && delta < bestDelta) {
      best = snapshot;
      bestDelta = delta;
    }
  });
  if (best) return best;
  return snapshots.filter((snapshot) => String(snapshot.data_date) < String(latestDate)).at(0) || null;
}

function periodWeeks(period) {
  if (!period) return [];
  if (period.weeks && period.weeks.length) return period.weeks;
  if (period.week && period.week !== "ALL") return [period.week];
  if (period.month) return weeksForMonth(period.month);
  return [];
}

function daysUntilPeriodEnd(period, currentDate) {
  const weeks = periodWeeks(period);
  if (!weeks.length) return 1;
  const end = weeks.map(koreanWeekToDate).sort((a, b) => a - b).at(-1);
  return Math.max(1, diffDays(currentDate, end));
}

function diffDays(start, end) {
  if (!start || !end) return 0;
  return Math.round((end - start) / 86400000);
}

function paceStatus({ gap, requiredDaily, pace3, paceRatio, projectedGap, bsaTeu }) {
  if (!bsaTeu) return "no-bsa";
  if (gap <= 0) return "filled";
  if (pace3 <= 0) return "stalled";
  if (paceRatio != null && paceRatio < .5) return "slow";
  if (paceRatio != null && paceRatio < .9) return "short";
  if (projectedGap <= Math.max(10, bsaTeu * .05)) return "ok";
  return "watch";
}

function buildShipperExceptions(currentMap, baselineMap, routeContext) {
  const keys = new Set([...currentMap.keys(), ...baselineMap.keys()]);
  const rows = [];

  keys.forEach((key) => {
    const current = currentMap.get(key);
    const base = baselineMap.get(key);
    const template = current || base;
    const currentTeu = current ? current.teu : 0;
    const baseTeu = base ? base.teu : 0;
    const delta = currentTeu - baseTeu;
    const deltaPct = baseTeu ? delta / baseTeu : currentTeu ? 1 : 0;
    const currentW3Teu = current ? current.w3Teu : 0;
    const baseW3Teu = base ? base.w3Teu : 0;
    const currentW3NormTeu = current ? current.w3NormTeu : 0;
    const baseW3NormTeu = base ? base.w3NormTeu : 0;
    const currentW3CancelTeu = current ? current.w3CancelTeu : 0;
    const baseW3CancelTeu = base ? base.w3CancelTeu : 0;
    const currentW3HiTeu = current ? current.w3HiTeu : 0;
    const baseW3HiTeu = base ? base.w3HiTeu : 0;
    const w3Delta = currentW3Teu - baseW3Teu;
    const w3DeltaPct = baseW3Teu ? w3Delta / baseW3Teu : currentW3Teu ? 1 : 0;

    const issue = classifyShipper({
      currentTeu,
      baseTeu,
      delta,
      deltaPct,
      currentW3Teu,
      baseW3Teu,
      currentW3NormTeu,
      baseW3NormTeu,
      currentW3CancelTeu,
      baseW3CancelTeu,
      currentW3HiTeu,
      baseW3HiTeu,
      w3Delta,
      w3DeltaPct
    });
    if (!issue) return;

    const impactTeu = Math.max(0, baseTeu - currentTeu, baseW3Teu - currentW3Teu);
    const context = routeContext.get(template.routeKey) || {};
    const bsaShortfall = Math.max(0, (context.bsaTeu || 0) - currentTeu);
    const focus = classifyShipperFocus(issue, context, impactTeu);
    rows.push({
      ...template,
      bsaTeu: context.bsaTeu || 0,
      bsaShortfall,
      isImportant: Boolean(context.isImportant),
      priority: focus.priority,
      focusReason: focus.reason,
      focusAction: focus.action,
      paceStatus: context.status || "no-history",
      pace3: context.pace3,
      requiredDaily: context.requiredDaily,
      projectedGap: context.projectedGap || 0,
      currentTeu,
      baseTeu,
      delta,
      deltaPct,
      currentW3Teu,
      baseW3Teu,
      currentW3NormTeu,
      baseW3NormTeu,
      currentW3CancelTeu,
      baseW3CancelTeu,
      currentW3HiTeu,
      baseW3HiTeu,
      w3Delta,
      w3DeltaPct,
      issue: issue.type,
      signals: issue.signals,
      level: issue.level,
      reason: issue.reason,
      action: issue.action,
      impactTeu,
      score: issue.score + Math.min(30, impactTeu) + importanceBonus(context, impactTeu) + focus.bonus
    });
  });

  return rows.sort((a, b) => b.score - a.score || b.impactTeu - a.impactTeu);
}

function classifyShipper(metrics) {
  const {
    currentTeu,
    baseTeu,
    delta,
    deltaPct,
    currentW3Teu,
    baseW3Teu,
    currentW3NormTeu,
    currentW3CancelTeu,
    currentW3HiTeu,
    baseW3HiTeu,
    w3Delta,
    w3DeltaPct
  } = metrics;
  const loss = Math.max(0, baseTeu - currentTeu);
  const w3Loss = Math.max(0, baseW3Teu - currentW3Teu);
  const hiW3Loss = Math.max(0, baseW3HiTeu - currentW3HiTeu);
  const w3CancelRate = currentW3Teu ? currentW3CancelTeu / currentW3Teu : 0;
  const w3SecuredRate = currentTeu ? currentW3NormTeu / currentTeu : 0;
  const lateShare = currentTeu ? Math.max(0, currentTeu - currentW3NormTeu) / currentTeu : 0;
  const candidates = [];

  if (baseTeu >= 5 && currentTeu === 0) {
    candidates.push({
      type: "이탈",
      level: "high",
      score: 85,
      reason: `기준 ${fmt(baseTeu)} TEU에서 현재 선적 없음`,
      action: "영업 확인 및 경쟁사/운임/선복 이슈 확인"
    });
  }
  if (baseTeu >= 8 && deltaPct <= -0.45 && loss >= 5) {
    candidates.push({
      type: "급감",
      level: "high",
      score: 76,
      reason: `TEU ${pct(deltaPct)} 감소`,
      action: "감소 사유 확인 후 회복 가능 물량 협의"
    });
  }
  if (baseTeu >= 5 && deltaPct <= -0.25 && loss >= 3) {
    candidates.push({
      type: "감소",
      level: "mid",
      score: 58,
      reason: `기준 대비 ${fmt(loss)} TEU 감소`,
      action: "담당 화주 선적 계획 재확인"
    });
  }
  if (baseW3Teu >= 5 && currentW3Teu === 0) {
    candidates.push({
      type: "3W 이탈",
      level: "high",
      score: 82,
      reason: `3주전 부킹 기준 ${fmt(baseW3Teu)} TEU에서 현재 0 TEU`,
      action: "차주 이후 선적 계획과 경쟁사 전환 여부 확인"
    });
  }
  if (baseW3Teu >= 8 && w3DeltaPct <= -0.45 && w3Loss >= 5) {
    candidates.push({
      type: "3W 급감",
      level: "high",
      score: 74,
      reason: `3주전 부킹 TEU ${pct(w3DeltaPct)} 감소`,
      action: "3주전 확보 물량 감소 원인과 보완 부킹 가능성 확인"
    });
  }
  if (baseW3HiTeu >= 5 && hiW3Loss >= 3 && hiW3Loss / baseW3HiTeu >= .3) {
    candidates.push({
      type: "3W 고수익 감소",
      level: "high",
      score: 72,
      reason: `고수익 3W 부킹 ${fmt(hiW3Loss)} TEU 감소`,
      action: "고수익 화주 선적 계획과 운임 조건 우선 점검"
    });
  }
  if (baseW3Teu >= 5 && w3DeltaPct <= -0.25 && w3Loss >= 3) {
    candidates.push({
      type: "3W 감소",
      level: "mid",
      score: 56,
      reason: `3주전 부킹 기준 대비 ${fmt(w3Loss)} TEU 감소`,
      action: "담당 화주 3주전 부킹 회복 가능성 확인"
    });
  }
  if (currentW3Teu >= 10 && currentW3CancelTeu >= 3 && w3CancelRate >= .15) {
    candidates.push({
      type: "3W 취소위험",
      level: "mid",
      score: 52,
      reason: `3W 취소율 ${rpct(w3CancelRate)}`,
      action: "부킹 품질과 취소 사유 확인"
    });
  }
  if (currentTeu >= 20 && lateShare >= .65 && w3SecuredRate <= .35) {
    candidates.push({
      type: "Late 의존",
      level: "watch",
      score: 42,
      reason: `3W 확보율 ${rpct(w3SecuredRate)}, Late 의존 ${rpct(lateShare)}`,
      action: "조기 부킹 유도와 선복 예측 리스크 확인"
    });
  }
  if (baseTeu === 0 && currentTeu >= 20) {
    candidates.push({
      type: "신규대형",
      level: "watch",
      score: 45,
      reason: `신규 ${fmt(currentTeu)} TEU 유입`,
      action: "거래 조건과 반복 가능성 확인"
    });
  }
  if (baseTeu >= 8 && currentTeu >= baseTeu * 2 && currentTeu - baseTeu >= 20) {
    candidates.push({
      type: "급증",
      level: "watch",
      score: 42,
      reason: `기준 대비 ${fmt(currentTeu - baseTeu)} TEU 증가`,
      action: "선복과 장비 대응 가능 여부 확인"
    });
  }
  if (baseW3Teu === 0 && currentW3Teu >= 20) {
    candidates.push({
      type: "3W 신규대형",
      level: "watch",
      score: 41,
      reason: `3주전 신규 ${fmt(currentW3Teu)} TEU 유입`,
      action: "반복 물량 여부와 선복 대응 가능성 확인"
    });
  }

  if (!candidates.length) return null;
  candidates.sort((a, b) => b.score - a.score);
  return {
    ...candidates[0],
    signals: candidates.map((candidate) => candidate.type).slice(0, 4)
  };
}

function buildRouteExceptions(currentRoutes, baselineRoutes, shipperExceptions, routeContext, currentBsaRoutes, baselineBsaRoutes) {
  const exceptionByRoute = new Map();
  shipperExceptions.forEach((row) => {
    const list = exceptionByRoute.get(row.routeKey) || [];
    list.push(row);
    exceptionByRoute.set(row.routeKey, list);
  });

  const keys = new Set([...currentRoutes.keys(), ...baselineRoutes.keys(), ...exceptionByRoute.keys(), ...currentBsaRoutes.keys(), ...baselineBsaRoutes.keys()]);
  const rows = [];

  keys.forEach((key) => {
    const current = currentRoutes.get(key);
    const base = baselineRoutes.get(key);
    const context = routeContext.get(key) || {};
    const currentBsa = currentBsaRoutes.get(key);
    const baseBsa = baselineBsaRoutes.get(key);
    const template = current || base || currentBsa || baseBsa || context;
    const currentTeu = current ? current.teu : 0;
    const baseTeu = base ? base.teu : 0;
    const bsaTeu = currentBsa ? currentBsa.bsaTeu : (context.bsaTeu || 0);
    const bsaShortfall = Math.max(0, bsaTeu - currentTeu);
    const delta = currentTeu - baseTeu;
    const deltaPct = baseTeu ? delta / baseTeu : currentTeu ? 1 : 0;
    const currentW3Teu = current ? current.w3Teu : 0;
    const baseW3Teu = base ? base.w3Teu : 0;
    const w3Delta = currentW3Teu - baseW3Teu;
    const w3DeltaPct = baseW3Teu ? w3Delta / baseW3Teu : currentW3Teu ? 1 : 0;
    const currentW3NormTeu = current ? current.w3NormTeu : 0;
    const baseW3NormTeu = base ? base.w3NormTeu : 0;
    const currentW3CancelTeu = current ? current.w3CancelTeu : 0;
    const currentW3HiTeu = current ? current.w3HiTeu : 0;
    const baseW3HiTeu = base ? base.w3HiTeu : 0;
    const w3HiDelta = currentW3HiTeu - baseW3HiTeu;
    const w3HiDeltaPct = baseW3HiTeu ? w3HiDelta / baseW3HiTeu : currentW3HiTeu ? 1 : 0;
    const currentShipperCount = current ? current.shippers.size : 0;
    const baseShipperCount = base ? base.shippers.size : 0;
    const shipperDelta = currentShipperCount - baseShipperCount;
    const currentW3ShipperCount = current ? current.w3Shippers.size : 0;
    const baseW3ShipperCount = base ? base.w3Shippers.size : 0;
    const w3ShipperDelta = currentW3ShipperCount - baseW3ShipperCount;
    const exceptionRows = exceptionByRoute.get(key) || [];
    const lost = exceptionRows.filter((row) => row.issue === "이탈").length;
    const down = exceptionRows.filter((row) => row.issue === "감소" || row.issue === "급감").length;
    const w3Lost = exceptionRows.filter((row) => hasSignal(row, "3W 이탈")).length;
    const w3Down = exceptionRows.filter((row) => hasSignal(row, "3W 감소") || hasSignal(row, "3W 급감")).length;
    const impactTeu = exceptionRows.reduce((sum, row) => sum + row.impactTeu, 0);
    const topShare = getTopShare(current);
    const w3SecuredRate = currentTeu ? currentW3NormTeu / currentTeu : 0;
    const baseW3SecuredRate = baseTeu ? baseW3NormTeu / baseTeu : 0;
    const lateShare = currentTeu ? Math.max(0, currentTeu - currentW3NormTeu) / currentTeu : 0;
    const w3CancelRate = currentW3Teu ? currentW3CancelTeu / currentW3Teu : 0;
    const bsaUtil = bsaTeu ? currentTeu / bsaTeu : 0;
    const w3BsaUtil = bsaTeu ? currentW3Teu / bsaTeu : 0;
    const actionProfile = buildRouteActionProfile(exceptionRows, context, bsaShortfall);
    const absoluteFactor = absoluteGapFactor(context);

    const issues = [];
    let score = 0;
    if (["trend-short", "trend-slow"].includes(context.leadTrendStatus) && (context.leadTrendGap || 0) >= Math.max(10, (context.portImpactThreshold || 30) * .45)) {
      issues.push("리드타임 트렌드 부족");
      score += context.leadTrendStatus === "trend-short" ? 34 : 22;
    }
    if (["stalled", "slow", "short"].includes(context.status) && (context.projectedGap || 0) >= 20) {
      issues.push(context.leadTrendStatus === "trend-ok" ? "BSA 목표 Gap" : "부킹속도 부족");
      score += context.status === "stalled" ? 36 : context.status === "slow" ? 30 : 22;
    }
    if ((context.projectedGap || 0) >= Math.max(30, bsaTeu * .08)) {
      issues.push("예상미달");
      score += 24;
    }
    if (bsaTeu >= 100 && bsaShortfall >= 20 && bsaUtil < .75) {
      issues.push("BSA 미달");
      score += horizonWeight("bsaShortfall", 34);
    }
    if (bsaTeu >= 100 && bsaTeu - currentW3Teu >= 30 && w3BsaUtil < .6) {
      issues.push("3W/BSA 낮음");
      score += horizonWeight("w3Bsa", 28);
    }
    if (baseTeu >= 10 && deltaPct <= -0.25 && Math.abs(delta) >= 8) {
      issues.push("TEU 감소");
      score += (30 + Math.min(20, Math.abs(deltaPct) * 30)) * absoluteFactor;
    }
    if (baseShipperCount >= 3 && shipperDelta <= -2) {
      issues.push("화주수 감소");
      score += 32 * absoluteFactor;
    }
    if (lost > 0) {
      issues.push("화주 이탈");
      score += (18 + Math.min(18, lost * 4)) * absoluteFactor;
    }
    if (down > 0) {
      issues.push("화주 물량 감소");
      score += (14 + Math.min(15, down * 3)) * absoluteFactor;
    }
    if (baseW3Teu >= 10 && w3DeltaPct <= -0.25 && Math.abs(w3Delta) >= 8) {
      issues.push("3W TEU 감소");
      score += 28 + Math.min(18, Math.abs(w3DeltaPct) * 28);
    }
    if (baseW3ShipperCount >= 3 && w3ShipperDelta <= -2) {
      issues.push("3W 화주수 감소");
      score += 28;
    }
    if (w3Lost > 0) {
      issues.push("3W 화주 이탈");
      score += 16 + Math.min(16, w3Lost * 4);
    }
    if (w3Down > 0) {
      issues.push("3W 화주 감소");
      score += 12 + Math.min(14, w3Down * 3);
    }
    if (baseW3HiTeu >= 10 && w3HiDeltaPct <= -0.3 && Math.abs(w3HiDelta) >= 5) {
      issues.push("3W 고수익 감소");
      score += 22;
    }
    if (baseW3SecuredRate - w3SecuredRate >= .15 && currentTeu >= 20) {
      issues.push("3W 확보율 하락");
      score += 18;
    }
    if (lateShare >= .55 && currentTeu >= 20) {
      issues.push("Late 의존");
      score += 12;
    }
    if (w3CancelRate >= .15 && currentW3CancelTeu >= 5) {
      issues.push("3W 취소율");
      score += 18;
    }
    if (topShare >= .55 && currentShipperCount <= 4 && currentTeu >= 20) {
      issues.push("화주 쏠림");
      score += 16;
    }
    if (!issues.length) return;

    score += importanceBonus(context, impactTeu);
    const focus = classifyRouteFocus({
      issues,
      context,
      impactTeu,
      bsaShortfall,
      currentTeu,
      baseTeu,
      currentW3Teu,
      bsaTeu,
      lost: lost + w3Lost,
      down: down + w3Down,
      actionProfile
    });
    score += focus.bonus;
    const level = score >= 65 ? "high" : score >= 40 ? "mid" : "watch";
    rows.push({
      ...template,
      bsaTeu,
      bsaUtil,
      w3BsaUtil,
      bsaShortfall,
      isImportant: Boolean(context.isImportant),
      priority: focus.priority,
      focusReason: focus.reason,
      focusAction: focus.action,
      actionDetail: actionProfile.label,
      paceStatus: context.status || "no-history",
      pace3: context.pace3,
      pace7: context.pace7,
      w3Pace3: context.w3Pace3,
      requiredDaily: context.requiredDaily,
      projectedTeu: context.projectedTeu,
      projectedGap: context.projectedGap || 0,
      daysRemaining: context.daysRemaining,
      leadTrendStatus: context.leadTrendStatus || "no-trend",
      leadTrendGap: context.leadTrendGap || 0,
      leadTrendLabel: context.leadTrendLabel || "",
      leadTrendExpectedRate: context.leadTrendExpectedRate || 0,
      currentTeu,
      baseTeu,
      delta,
      deltaPct,
      currentW3Teu,
      baseW3Teu,
      w3Delta,
      w3DeltaPct,
      currentW3NormTeu,
      baseW3NormTeu,
      currentW3CancelTeu,
      currentW3HiTeu,
      baseW3HiTeu,
      w3SecuredRate,
      baseW3SecuredRate,
      lateShare,
      w3CancelRate,
      currentShipperCount,
      baseShipperCount,
      shipperDelta,
      currentW3ShipperCount,
      baseW3ShipperCount,
      w3ShipperDelta,
      lost,
      down,
      w3Lost,
      w3Down,
      impactTeu,
      topShare,
      issues,
      level,
      score: Math.round(score)
    });
  });

  return rows.sort((a, b) => b.score - a.score || b.impactTeu - a.impactTeu);
}

function buildSalesActions(shipperExceptions, routeExceptions, currentRows, baselineRows) {
  const currentBySales = aggregateSalesTeu(currentRows);
  const baseBySales = aggregateSalesTeu(baselineRows);
  const currentW3BySales = aggregateSalesMetric(currentRows, "w3Teu");
  const baseW3BySales = aggregateSalesMetric(baselineRows, "w3Teu");
  const routeByKey = new Map(routeExceptions.map((row) => [row.routeKey, row]));
  const map = new Map();

  shipperExceptions.forEach((row) => {
    const found = map.get(row.sales) || {
      sales: row.sales,
      shippers: new Set(),
      routes: new Set(),
      routeImpacts: new Map(),
      routeStatusSeen: new Set(),
      impactTeu: 0,
      bsaGap: 0,
      projectedGap: 0,
      trendGap: 0,
      high: 0,
      p1: 0,
      p2: 0,
      recovery: 0,
      winBack: 0,
      advance: 0,
      protect: 0,
      trendRoutes: 0,
      speedRoutes: 0,
      bsaRoutes: 0,
      groups: new Map(),
      issues: new Map(),
      score: 0
    };
    found.shippers.add(row.shipperCode || row.shipperName);
    found.routes.add(row.routeKey);
    found.routeImpacts.set(row.routeKey, (found.routeImpacts.get(row.routeKey) || 0) + row.impactTeu);
    found.impactTeu += row.impactTeu;
    found.high += row.level === "high" ? 1 : 0;
    found.p1 += row.priority === "P1" ? 1 : 0;
    found.p2 += row.priority === "P2" ? 1 : 0;
    if (row.currentTeu > 0 && row.baseTeu > row.currentTeu) found.recovery += 1;
    if (row.currentTeu === 0 && row.baseTeu > 0) found.winBack += 1;
    if ((row.baseW3Teu || 0) > (row.currentW3Teu || 0)) found.advance += 1;
    if (hasSignal(row, "Late 의존") || hasSignal(row, "3W 취소위험")) found.protect += 1;
    (row.signals || [row.issue]).forEach((issue) => {
      found.issues.set(issue, (found.issues.get(issue) || 0) + 1);
      const group = riskGroup(issue);
      found.groups.set(group, (found.groups.get(group) || 0) + 1);
    });
    const route = routeByKey.get(row.routeKey);
    if (route && !found.routeStatusSeen.has(row.routeKey)) {
      found.routeStatusSeen.add(row.routeKey);
      found.bsaGap += route.bsaShortfall || 0;
      found.projectedGap += route.projectedGap || 0;
      found.trendGap += route.leadTrendGap || 0;
      found.trendRoutes += ["trend-short", "trend-slow"].includes(route.leadTrendStatus) ? 1 : 0;
      found.speedRoutes += ["stalled", "slow", "short"].includes(route.paceStatus) ? 1 : 0;
      found.bsaRoutes += route.bsaTeu > 0 ? 1 : 0;
    }
    found.score += row.score;
    map.set(row.sales, found);
  });

  return Array.from(map.values()).map((row) => {
    const currentTeu = currentBySales.get(row.sales) || 0;
    const baseTeu = baseBySales.get(row.sales) || 0;
    const currentW3Teu = currentW3BySales.get(row.sales) || 0;
    const baseW3Teu = baseW3BySales.get(row.sales) || 0;
    return {
      ...row,
      currentTeu,
      baseTeu,
      delta: currentTeu - baseTeu,
      currentW3Teu,
      baseW3Teu,
      w3Delta: currentW3Teu - baseW3Teu,
      actionGap: Math.max(row.bsaGap, row.projectedGap, row.trendGap, row.impactTeu),
      topIssue: topIssue(row.issues),
      topRiskGroup: topIssue(row.groups),
      riskMix: topEntries(row.groups, 2).map(([group, count]) => `${riskGroupLabel(group)} ${fmt(count)}`).join(" · "),
      topRouteLabel: salesTopRoute(row),
      statusLabel: salesStatusLabel(row),
      statusTone: salesStatusTone(row),
      focusAction: salesFocusAction(row)
    };
  }).sort((a, b) => b.impactTeu - a.impactTeu || b.high - a.high);
}

function topEntries(map, limit) {
  return Array.from(map.entries()).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, limit);
}

function salesTopRoute(row) {
  const best = topEntries(row.routeImpacts, 1)[0];
  if (!best) return "-";
  const [routeKey, impact] = best;
  const parts = routeKey.split("|");
  if (parts.length < 4) return `${routeKey} ${fmt(impact)}TEU`;
  return `${parts[0]} ${parts[1]}→${parts[2]} ${parts[3]} · ${fmt(impact)}TEU`;
}

function salesStatusCode(row) {
  if (row.p1 >= 8 || row.trendRoutes >= 5 || row.bsaGap >= 500) return "즉시 대응";
  if (row.p1 > 0 || row.bsaGap >= 150 || row.trendRoutes > 0) return "우선 확인";
  if (row.winBack || row.recovery) return "화주 회복";
  return "관찰";
}

function salesStatusLabel(row) {
  const code = salesStatusCode(row);
  if (state.lang === "en") {
    if (code === "즉시 대응") return "Immediate";
    if (code === "우선 확인") return "Priority Check";
    if (code === "화주 회복") return "Customer Recovery";
    return "Watch";
  }
  return code;
}

function salesStatusTone(row) {
  const code = salesStatusCode(row);
  if (code === "즉시 대응") return "neg";
  if (code === "우선 확인") return "warn";
  if (code === "화주 회복") return "blue";
  return "pos";
}

function salesFocusAction(row) {
  const code = salesStatusCode(row);
  if (state.lang === "en") {
    if (row.trendRoutes && row.bsaGap > 0) return "Start with delayed trend routes and separate recovery from substitute volume.";
    if (row.bsaGap >= 150) return "Split the BSA gap into recoverable customer volume and substitute volume.";
    if (code === "우선 확인" && row.winBack > row.recovery) return "Prioritize win-back feasibility and confirm competitor-switch reasons.";
    if (code === "우선 확인" && row.recovery > 0) return "Check recoverable residual plans for declining customers first.";
    if (code === "화주 회복" && row.recovery > 0) return "Confirm shipment-plan changes and recoverable TEU with declining customers.";
    if (code === "화주 회복" && row.winBack > 0) return "Win back lost repeat customers after checking rate, space, and schedule reasons.";
    if (row.advance > 0) return "Convert missing 3W advance-booking customers earlier.";
    return "Review the top route and customer state before outreach.";
  }
  if (row.trendRoutes && row.bsaGap > 0) return "트렌드 지연 구간의 회복/대체 후보를 먼저 확인";
  if (row.bsaGap >= 150) return "관련 BSA Gap을 회복 후보와 대체 물량으로 분리";
  if (code === "우선 확인" && row.winBack > row.recovery) return "이탈 화주 재확보 가능성과 경쟁사 전환 사유를 우선 확인";
  if (code === "우선 확인" && row.recovery > 0) return "감소 화주의 회복 가능 잔여 물량을 먼저 확인";
  if (code === "화주 회복" && row.recovery > 0) return "감소 화주의 선적 계획 변경과 회복 TEU 확인";
  if (code === "화주 회복" && row.winBack > 0) return "반복 이탈 화주의 운임/선복/스케줄 이슈 확인 후 재확보";
  if (row.advance > 0) return "3W 선행부킹 미유입 화주를 조기 전환";
  return "주요 변동 구간 상태 확인";
}

function summarizeIssues(shipperExceptions, routeExceptions) {
  const issueMap = new Map();
  const bump = (issue, impactTeu, high) => {
    const found = issueMap.get(issue) || { issue, count: 0, impactTeu: 0, high: 0 };
    found.count += 1;
    found.impactTeu += impactTeu;
    found.high += high;
    issueMap.set(issue, found);
  };

  shipperExceptions.forEach((row) => {
    (row.signals || [row.issue]).forEach((issue) => {
      bump(issue, issue === row.issue ? row.impactTeu : 0, row.level === "high" && issue === row.issue ? 1 : 0);
    });
  });

  routeExceptions.forEach((row) => {
    row.issues.forEach((issue) => {
      if (!issueMap.has(issue)) {
        bump(issue, 0, row.level === "high" ? 1 : 0);
      }
    });
  });

  return Array.from(issueMap.values()).sort((a, b) => b.impactTeu - a.impactTeu || b.count - a.count);
}

function aggregateSalesTeu(rows) {
  const map = new Map();
  rows.forEach((row) => {
    map.set(row.sales, (map.get(row.sales) || 0) + row.teu);
  });
  return map;
}

function aggregateSalesMetric(rows, field) {
  const map = new Map();
  rows.forEach((row) => {
    map.set(row.sales, (map.get(row.sales) || 0) + (row[field] || 0));
  });
  return map;
}

function applyPriorityFilter(rows) {
  if (state.filters.priority !== "important") return rows;
  return rows.filter((row) => row.isImportant && row.priority !== "P3");
}

function importanceBonus(context, impactTeu) {
  if (!context) return impactTeu >= 20 ? 8 : 0;
  let bonus = 0;
  const volume = Math.max(context.currentTeu || 0, context.baseTeu || 0, context.currentW3Teu || 0, context.baseW3Teu || 0);
  const bsaThreshold = context.portBsaThreshold || 100;
  const volumeThreshold = context.portVolumeThreshold || 50;
  const impactThreshold = context.portImpactThreshold || 30;
  if ((context.bsaTeu || 0) >= bsaThreshold * 1.5) bonus += 18;
  else if ((context.bsaTeu || 0) >= bsaThreshold) bonus += 10;
  if (volume >= volumeThreshold * 1.5) bonus += 10;
  else if (volume >= volumeThreshold) bonus += 6;
  if (impactTeu >= impactThreshold * 1.5) bonus += 10;
  else if (impactTeu >= impactThreshold) bonus += 6;
  if ((context.bsaTeu || 0) > 0 && impactTeu / context.bsaTeu >= .1) bonus += 8;
  return bonus;
}

function horizonWeight(metric, base) {
  const mode = state.filters.horizon;
  if (metric === "bsaShortfall" && mode === "w1") return base * 1.25;
  if (metric === "bsaShortfall" && mode === "w2") return base * 1.08;
  if (metric === "bsaShortfall" && mode === "w3") return base * .72;
  if (metric === "w3Bsa" && mode === "w3") return base * 1.25;
  if (metric === "w3Bsa" && mode === "w2") return base * 1.12;
  return base;
}

function absoluteGapFactor(context) {
  const weeks = context.leadTrendWeeks || [];
  if (!weeks.length) return state.filters.horizon === "w3" ? .45 : state.filters.horizon === "w2" ? .7 : 1;
  const nearest = weeks.reduce((min, week) => Math.min(min, week.leadOffset), Infinity);
  if (nearest <= 1) return 1;
  if (nearest === 2) return .7;
  if (nearest === 3) return .45;
  return .35;
}

function buildRouteActionProfile(exceptionRows, context, bsaShortfall) {
  const minImpact = Math.max(3, Math.min(20, (context.portImpactThreshold || 30) * .2));
  const actionableRows = exceptionRows.filter((row) => row.impactTeu >= minImpact);
  const recovery = actionableRows.filter((row) => row.currentTeu > 0 && row.baseTeu > row.currentTeu);
  const winBack = actionableRows.filter((row) => row.currentTeu === 0 && row.baseTeu > 0);
  const advance = actionableRows.filter((row) => row.currentTeu > 0 && row.baseW3Teu > row.currentW3Teu && (row.baseW3Teu - row.currentW3Teu) >= minImpact);
  const protect = exceptionRows.filter((row) => hasSignal(row, "Late 의존") || hasSignal(row, "3W 취소위험"));
  const candidateTeu = sumTopImpact(recovery, 5) + sumTopImpact(winBack, 5) + sumTopW3Impact(advance, 5);
  const alternativeNeeded = bsaShortfall > Math.max(candidateTeu, context.portImpactThreshold || 30);
  const labels = [];

  if (recovery.length) labels.push(`회복 ${Math.min(5, recovery.length)}곳/${fmt(sumTopImpact(recovery, 5))}TEU`);
  if (winBack.length) labels.push(`재확보 ${Math.min(5, winBack.length)}곳/${fmt(sumTopImpact(winBack, 5))}TEU`);
  if (advance.length) labels.push(`선행 ${Math.min(5, advance.length)}곳/${fmt(sumTopW3Impact(advance, 5))}TEU`);
  if (protect.length) labels.push(`방어 ${Math.min(5, protect.length)}곳`);
  if (alternativeNeeded) labels.push("대체 필요");

  return {
    recoveryCount: recovery.length,
    winBackCount: winBack.length,
    advanceCount: advance.length,
    protectCount: protect.length,
    candidateTeu,
    alternativeNeeded,
    label: labels.join(" · ")
  };
}

function sumTopImpact(rows, limit) {
  return rows
    .slice()
    .sort((a, b) => b.impactTeu - a.impactTeu)
    .slice(0, limit)
    .reduce((sum, row) => sum + row.impactTeu, 0);
}

function sumTopW3Impact(rows, limit) {
  return rows
    .slice()
    .sort((a, b) => (b.baseW3Teu - b.currentW3Teu) - (a.baseW3Teu - a.currentW3Teu))
    .slice(0, limit)
    .reduce((sum, row) => sum + Math.max(0, row.baseW3Teu - row.currentW3Teu), 0);
}

function routeAction(actionProfile, fallback) {
  if (actionProfile.recoveryCount && actionProfile.winBackCount) return "회복/재확보 후보 우선 접촉";
  if (actionProfile.recoveryCount) return "회복 가능 물량 우선 협의";
  if (actionProfile.winBackCount) return "이탈 화주 재확보 가능성 확인";
  if (actionProfile.advanceCount) return "선행부킹 전환 후보 확정";
  if (actionProfile.protectCount) return "현재 물량 방어 조건 확인";
  if (actionProfile.alternativeNeeded) return "대체 물량 또는 BSA 조정 판단";
  return fallback;
}

function classifyRouteFocus({ issues, context, impactTeu, bsaShortfall, currentTeu, baseTeu, currentW3Teu, bsaTeu, lost, down, actionProfile }) {
  const projectedGap = context.projectedGap || 0;
  const bsaThreshold = context.portBsaThreshold || 100;
  const impactThreshold = context.portImpactThreshold || 30;
  const speedBad = ["stalled", "slow", "short"].includes(context.status);
  const trendNormal = context.leadTrendStatus === "trend-ok";
  const leadOffset = context.leadTrendWorst ? context.leadTrendWorst.leadOffset : null;
  if (["trend-short", "trend-slow"].includes(context.leadTrendStatus) && (context.leadTrendGap || 0) >= Math.max(impactThreshold, bsaTeu * .08)) {
    return {
      priority: context.leadTrendStatus === "trend-short" ? "P1" : "P2",
      reason: "리드타임 트렌드 부족",
      action: routeAction(actionProfile, "리드타임별 회복 후보 확인"),
      bonus: context.leadTrendStatus === "trend-short" ? 30 : 18
    };
  }
  if (bsaTeu >= bsaThreshold && trendNormal && speedBad && projectedGap >= Math.max(impactThreshold, bsaTeu * .15)) {
    return {
      priority: leadOffset != null && leadOffset <= 1 ? "P1" : "P2",
      reason: "BSA 목표 Gap",
      action: routeAction(actionProfile, "BSA 적정성/대체 물량 판단"),
      bonus: leadOffset != null && leadOffset <= 1 ? 28 : 18
    };
  }
  if (bsaTeu >= bsaThreshold && speedBad && projectedGap >= Math.max(impactThreshold, bsaTeu * .15)) {
    return {
      priority: "P1",
      reason: "BSA 속도 부족",
      action: routeAction(actionProfile, "BSA Gap 기여 후보 선별"),
      bonus: 34
    };
  }
  if (bsaTeu >= bsaThreshold && bsaShortfall >= Math.max(impactThreshold, bsaTeu * .25) && currentTeu / bsaTeu < .5) {
    return {
      priority: "P1",
      reason: "BSA 미달",
      action: routeAction(actionProfile, "Gap 회복/대체 물량 병행"),
      bonus: 28
    };
  }
  if (impactTeu >= Math.max(50, impactThreshold) && (lost > 0 || down > 2)) {
    return {
      priority: "P1",
      reason: "핵심 화주 감소",
      action: routeAction(actionProfile, "핵심 화주 회복 가능성 확인"),
      bonus: 24
    };
  }
  if (bsaTeu >= bsaThreshold && currentW3Teu / bsaTeu < .55 && state.filters.horizon !== "w1") {
    return {
      priority: "P2",
      reason: "3W 확보 부족",
      action: routeAction(actionProfile, "선행부킹 미유입 화주 확인"),
      bonus: 18
    };
  }
  if (Math.max(currentTeu, baseTeu, currentW3Teu) >= 50 || impactTeu >= 20 || issues.length) {
    return {
      priority: "P2",
      reason: issues[0] || "물량 변동",
      action: routeAction(actionProfile, "변동 사유와 회복 가능성 확인"),
      bonus: 8
    };
  }
  return {
    priority: "P3",
    reason: issues[0] || "참고",
    action: "추세 관찰",
    bonus: 0
  };
}

function classifyShipperFocus(issue, context, impactTeu) {
  const projectedGap = context.projectedGap || 0;
  const bsaTeu = context.bsaTeu || 0;
  const bsaThreshold = context.portBsaThreshold || 100;
  const impactThreshold = context.portImpactThreshold || 30;
  const speedBad = ["stalled", "slow", "short"].includes(context.status);
  const trendNormal = context.leadTrendStatus === "trend-ok";
  if (["trend-short", "trend-slow"].includes(context.leadTrendStatus) && (context.leadTrendGap || 0) >= impactThreshold && impactTeu >= Math.max(10, impactThreshold * .2)) {
    return {
      priority: context.leadTrendStatus === "trend-short" ? "P1" : "P2",
      reason: "리드타임 트렌드 부족 구간의 감소 화주",
      action: "회복/선행부킹 가능성 확인",
      bonus: context.leadTrendStatus === "trend-short" ? 22 : 12
    };
  }
  if (bsaTeu >= bsaThreshold && trendNormal && speedBad && projectedGap >= impactThreshold && impactTeu >= Math.max(15, impactThreshold * .25)) {
    return {
      priority: "P2",
      reason: "BSA 목표 Gap 구간의 감소 화주",
      action: "Gap 기여 회복 가능 TEU 확인",
      bonus: 14
    };
  }
  if (bsaTeu >= bsaThreshold && speedBad && projectedGap >= impactThreshold && impactTeu >= Math.max(15, impactThreshold * .25)) {
    return {
      priority: "P1",
      reason: "BSA 속도 부족 구간의 감소 화주",
      action: "회복 가능 TEU 확인",
      bonus: 24
    };
  }
  if (issue.level === "high" && impactTeu >= 50) {
    return {
      priority: "P1",
      reason: issue.type,
      action: issue.action,
      bonus: 18
    };
  }
  if (bsaTeu >= Math.max(270, bsaThreshold * 1.5) && impactTeu >= impactThreshold) {
    return {
      priority: "P1",
      reason: issue.type,
      action: issue.action,
      bonus: 14
    };
  }
  if (bsaTeu >= bsaThreshold && impactTeu >= 5) {
    return {
      priority: "P2",
      reason: issue.type,
      action: issue.action,
      bonus: 10
    };
  }
  if (impactTeu >= 10 || issue.level === "mid") {
    return {
      priority: "P2",
      reason: issue.type,
      action: issue.action,
      bonus: 6
    };
  }
  return {
    priority: "P3",
    reason: issue.type,
    action: "추세 관찰",
    bonus: 0
  };
}

function renderKpis(analysis) {
  const t = analysis.totals;
  const w3Pct = t.totalBaseW3Teu ? t.deltaW3Teu / t.totalBaseW3Teu : 0;
  const bsaUtil = t.totalBsaTeu ? t.totalCurrentTeu / t.totalBsaTeu : 0;
  const w3BsaUtil = t.totalBsaTeu ? t.totalCurrentW3Teu / t.totalBsaTeu : 0;
  const bsaShortfall = Math.max(0, t.totalBsaTeu - t.totalCurrentTeu);
  const kpis = [
    {
      key: "totalTeu",
      label: state.lang === "en" ? "Total TEU" : "총 TEU",
      value: fmt(t.totalCurrentTeu),
      note: state.lang === "en" ? `${analysis.periods.baselineLabel} ${signed(t.deltaTeu)} TEU` : `${analysis.periods.baselineLabel} 대비 ${signed(t.deltaTeu)} TEU`,
      tone: t.deltaTeu < 0 ? "neg" : "pos"
    },
    {
      key: "bsaUtil",
      label: state.lang === "en" ? "TEU vs BSA" : "BSA 대비 TEU",
      value: t.totalBsaTeu ? rpct(bsaUtil) : "-",
      note: `BSA ${fmt(t.totalBsaTeu)} TEU · Gap ${fmt(bsaShortfall)}`,
      tone: t.totalBsaTeu && bsaUtil < .75 ? "neg" : "pos"
    },
    {
      key: "paceRisk",
      label: state.lang === "en" ? "Trend/Pace Risk" : "트렌드/속도 부족",
      value: fmt(t.speedRiskRoutes),
      note: state.lang === "en" ? `${historyLabel()} · projected gap ${fmt(t.projectedGapTeu)} TEU` : `${historyLabel()} · 예상미달 ${fmt(t.projectedGapTeu)} TEU`,
      tone: t.speedRiskRoutes ? "neg" : "pos"
    },
    {
      key: "topAction",
      label: state.lang === "en" ? "Top Actions" : "오늘 Top 조치",
      value: fmt(t.topActionCount),
      note: state.lang === "en" ? `P1 candidates ${fmt(t.p1Actions)} · top rows shown` : `P1 후보 ${fmt(t.p1Actions)}건 · 상위만 표시`,
      tone: t.topActionCount ? "warn" : "pos"
    },
    {
      key: "w3Teu",
      label: "3W Booking TEU",
      value: fmt(t.totalCurrentW3Teu),
      note: state.lang === "en" ? `${analysis.periods.baselineLabel} ${signed(t.deltaW3Teu)} TEU, ${pct(w3Pct)}` : `${analysis.periods.baselineLabel} 대비 ${signed(t.deltaW3Teu)} TEU, ${pct(w3Pct)}`,
      tone: t.deltaW3Teu < 0 ? "neg" : "pos"
    },
    {
      key: "w3Bsa",
      label: "3W/BSA",
      value: t.totalBsaTeu ? rpct(w3BsaUtil) : "-",
      note: state.lang === "en" ? `3W active ${fmt(t.w3ActiveCurrent)} customers` : `3W Active ${fmt(t.w3ActiveCurrent)}화주`,
      tone: t.totalBsaTeu && w3BsaUtil < .6 ? "warn" : "pos"
    },
    {
      key: "issueCustomers",
      label: state.lang === "en" ? "Declining/Lost Customers" : "감소/이탈 화주",
      value: fmt(t.issueShippers),
      note: state.lang === "en" ? `actionable out of ${fmt(t.allIssueShippers)} total` : `전체 ${fmt(t.allIssueShippers)}건 중 조치대상`,
      tone: t.issueShippers ? "warn" : "pos"
    },
    {
      key: "impactTeu",
      label: state.lang === "en" ? "Risk Impact TEU" : "문제 영향 TEU",
      value: fmt(t.impactTeu),
      note: state.lang === "en" ? `BSA-route impact ${fmt(t.bsaImpactTeu)} TEU` : `BSA 구간 영향 ${fmt(t.bsaImpactTeu)} TEU`,
      tone: t.impactTeu ? "neg" : "pos"
    },
    {
      key: "salesOwners",
      label: state.lang === "en" ? "Sales Owners" : "조치 영업사원",
      value: fmt(t.actionSales),
      note: state.lang === "en" ? `${fmt(t.highRoutes)} high routes` : `High 구간 ${fmt(t.highRoutes)}개`,
      tone: t.actionSales ? "warn" : "pos"
    }
  ];

  els.kpiGrid.innerHTML = kpis.map((item) => `
    <article class="kpi" title="${escapeAttr(kpiHelp(item.key))}">
      <div class="label">${item.label}</div>
      <div class="value ${item.tone}">${item.value}</div>
      <div class="note">${item.note}</div>
    </article>
  `).join("");
}

function renderRoutes(analysis) {
  const rows = analysis.routeExceptions.filter((row) => {
    if (state.riskFilter === "high") return row.level === "high";
    if (state.riskFilter === "watch") return row.level !== "high";
    return true;
  }).slice(0, 45);

  const baselineNote = analysis.periods.baseline && analysis.periods.baseline.leadAligned
    ? (state.lang === "en" ? "lead-time aligned baseline TEU" : "기준 TEU 리드타임 보정")
    : (state.lang === "en" ? "raw baseline TEU" : "기준 TEU 원자료");
  const routeMode = state.filters.priority === "important"
    ? (state.lang === "en" ? "destination-port p75 focus routes" : "도착포트별 p75 중요구간")
    : t("options.all");
  els.routeSubtitle.textContent = state.lang === "en"
    ? `${analysis.periods.label} vs ${analysis.periods.baselineLabel} · ${fmt(analysis.routeExceptions.length)} risk routes · ${baselineNote} · lead-time trend + gap judgment · ${routeMode}`
    : `${analysis.periods.label} vs ${analysis.periods.baselineLabel} · 문제 구간 ${fmt(analysis.routeExceptions.length)}개 · ${baselineNote} · 리드타임 트렌드+Gap 판단 · ${routeMode}`;

  if (!rows.length) {
    els.routeTable.innerHTML = emptyRow(7, state.lang === "en" ? "No exceptions for the selected filters." : "선택 조건에서 특이사항이 없습니다.");
    return;
  }

  els.routeTable.innerHTML = rows.map((row) => `
    <tr>
      <td>
        <div class="route-main">
          <div class="route-title">
            <span class="badge ${priorityClass(row.priority)}" title="${escapeAttr(priorityTitle(row.priority))}">${row.priority}</span>
            <strong>${row.origin} ${row.pol} → ${row.dest} ${row.dst}</strong>
          </div>
          <span class="subline">${t("labels.baseline")} ${fmt(row.baseTeu)} TEU · ${t("labels.change")} ${signed(row.delta)} · ${t("labels.impact")} ${fmt(row.impactTeu)} TEU</span>
        </div>
      </td>
      <td class="num">${fmt(row.currentTeu)}<br><span class="subline">${fmt(row.currentShipperCount)}${t("labels.shippers")} ${signed(row.shipperDelta)}</span></td>
      <td class="num">${row.bsaTeu ? fmt(row.bsaTeu) : "-"}<br><span class="subline">${row.bsaTeu ? `소석 ${rpct(row.bsaUtil)} · 예상Gap ${fmt(row.projectedGap)}` : ""}</span></td>
      <td>${paceCell(row)}</td>
      <td>
        <div class="metric-pair">
          <span>${fmt(row.currentW3Teu)} TEU <span class="${row.w3Delta < 0 ? "neg" : "pos"}">${signed(row.w3Delta)}</span></span>
          <span class="subline">${fmt(row.currentW3ShipperCount)}${t("labels.shippers")} <span class="${row.w3ShipperDelta < 0 ? "neg" : "pos"}">${signed(row.w3ShipperDelta)}</span> · ${t("labels.secured")} ${rpct(row.w3SecuredRate)}</span>
          <span class="subline">${t("labels.late")} ${rpct(row.lateShare)}</span>
        </div>
      </td>
      <td class="action-cell">
        <div class="focus-text">${actionText(row.focusAction)}</div>
        ${row.actionDetail ? `<div class="subline action-note">${actionDetailText(row.actionDetail)}</div>` : ""}
      </td>
      <td class="judgment-cell">
        ${issueChip(row.focusReason)}
        <div class="subline judgment-text">${riskMeaning(row.focusReason)}</div>
      </td>
    </tr>
  `).join("");
}

function renderSales(analysis) {
  const rows = analysis.salesActions.slice(0, 18);
  els.salesSubtitle.textContent = state.lang === "en"
    ? `${fmt(analysis.salesActions.length)} owners · owner status/gap basis`
    : `조치 대상 ${fmt(analysis.salesActions.length)}명 · 담당 상태/Gap 기준`;

  if (!rows.length) {
    els.salesTable.innerHTML = `<div class="empty-row">${state.lang === "en" ? "No sales owners require action." : "조치 대상 영업사원이 없습니다."}</div>`;
    return;
  }

  els.salesTable.innerHTML = rows.map((row) => `
    <article class="sales-card">
      <div class="sales-top">
        <div>
          <strong>${row.sales}</strong>
          <div class="subline">${fmt(row.routes.size)} ${t("labels.routes")} · ${fmt(row.shippers.size)}${t("labels.shippers")} · P1 ${fmt(row.p1)} / P2 ${fmt(row.p2)}</div>
        </div>
        <span class="status-pill ${row.statusTone}" title="${escapeAttr(salesStatusTitle(row))}">${row.statusLabel}</span>
      </div>
      <div class="sales-metrics">
        <div>
          <span class="metric-label" title="${escapeAttr(kpiHelp("impactTeu"))}">${t("labels.impactTeu")}</span>
          <strong class="neg">${fmt(row.impactTeu)} TEU</strong>
        </div>
        <div>
          <span class="metric-label" title="${escapeAttr(state.lang === "en" ? "The larger of BSA gap, projected gap, trend gap, and customer impact TEU." : "BSA Gap, 예상 Gap, 트렌드 Gap, 화주 영향 TEU 중 영업 관리 기준이 되는 값입니다.")}">${t("labels.actionGap")}</span>
          <strong>${fmt(row.actionGap)}</strong>
        </div>
        <div>
          <span class="metric-label" title="${escapeAttr(state.lang === "en" ? "Number of routes behind lead-time trend benchmarks." : "리드타임 트렌드 기준보다 느린 구간 수입니다.")}">${t("labels.trend")}</span>
          <strong>${fmt(row.trendRoutes)}${state.lang === "en" ? "" : "구간"}</strong>
        </div>
        <div>
          <span class="metric-label" title="${escapeAttr(state.lang === "en" ? "Recovery candidates have reduced volume; win-back candidates had baseline volume but no current volume." : "회복은 일부 물량이 남은 감소 화주, 재확보는 현재 0인 이탈 화주입니다.")}">${t("labels.customerState")}</span>
          <strong>${fmt(row.recovery)}${state.lang === "en" ? " recovery" : "회복"}/${fmt(row.winBack)}${state.lang === "en" ? " win-back" : "재확보"}</strong>
        </div>
      </div>
      <div class="sales-focus">${row.focusAction}</div>
      <div class="subline">${riskMixText(row.riskMix, row.topIssue)} · ${row.topRouteLabel}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${Math.min(100, row.score / 10)}%"></div></div>
    </article>
  `).join("");
}

function renderShippers(analysis) {
  const rows = analysis.shipperExceptions.slice(0, 60);
  els.shipperSubtitle.textContent = state.lang === "en"
    ? `Top ${fmt(rows.length)} rows · ${state.filters.priority === "important" ? "BSA/large-volume focus routes" : "all routes"}`
    : `상위 ${fmt(rows.length)}건 표시 · ${state.filters.priority === "important" ? "BSA/대형물량 중요구간 기준" : "전체 구간 기준"}`;

  if (!rows.length) {
    els.shipperTable.innerHTML = emptyRow(10, state.lang === "en" ? "No customer-level action candidates." : "화주 단위 조치 대상이 없습니다.");
    return;
  }

  els.shipperTable.innerHTML = rows.map((row) => `
    <tr>
      <td><span class="badge ${priorityClass(row.priority)}" title="${escapeAttr(priorityTitle(row.priority))}">${row.priority}</span></td>
      <td>
        <div class="shipper-main">
          <strong title="${escapeHtml(row.shipperName)}">${row.shipperName}</strong>
          <span class="subline">${row.shipperCode || "-"}</span>
        </div>
      </td>
      <td>${row.sales}</td>
      <td>
        ${row.origin} ${row.pol} → ${row.dest} ${row.dst}
        <div class="subline">${row.bsaTeu ? `BSA ${fmt(row.bsaTeu)} · Gap ${fmt(row.bsaShortfall)}` : t("labels.bsaNone")}</div>
      </td>
      <td class="num">${fmt(row.currentTeu)}</td>
      <td class="num">${fmt(row.baseTeu)}</td>
      <td class="num ${row.delta < 0 ? "neg" : "pos"}">${signed(row.delta)}<br><span class="subline">${pct(row.deltaPct)}</span></td>
      <td class="num">${fmt(row.currentW3Teu)}<br><span class="${row.w3Delta < 0 ? "neg" : "pos"}">${signed(row.w3Delta)}</span></td>
      <td class="action-cell">${actionText(row.focusAction)}</td>
      <td class="judgment-cell">${issueChip(row.focusReason)}<div class="subline judgment-text">${riskMeaning(row.focusReason)} · ${actionText(row.reason)}</div></td>
    </tr>
  `).join("");
}

function renderIssues(analysis) {
  const rows = analysis.issueSummary;
  const groups = groupIssueSummary(rows);
  els.issueSubtitle.textContent = state.lang === "en"
    ? `${fmt(groups.length)} groups · ${fmt(rows.length)} detailed types`
    : `${fmt(groups.length)}개 상위 유형 · 세부 ${fmt(rows.length)}개`;

  if (!groups.length) {
    els.issueList.innerHTML = `<div class="empty-row">${state.lang === "en" ? "No risk types." : "특이사항 유형이 없습니다."}</div>`;
    return;
  }

  els.issueList.innerHTML = groups.map((row) => `
    <div class="issue-item">
      <div class="top">
        <span>${riskGroupLabel(row.group)}</span>
        <span>${fmt(row.count)}${state.lang === "en" ? "" : "건"}</span>
      </div>
      <div class="risk-guide">${row.meaning}</div>
      <div class="desc">
        ${row.approach} · ${state.lang === "en" ? "Impact" : "영향"} ${fmt(row.impactTeu)} TEU · High ${fmt(row.high)}
      </div>
      <div class="risk-subchips">${row.children.map((child) => issueChip(child.issue, ` ${fmt(child.count)}`)).join("")}</div>
    </div>
  `).join("");
}

function renderError(message) {
  els.kpiGrid.innerHTML = `
    <article class="kpi">
      <div class="label">Error</div>
      <div class="value neg">Load failed</div>
      <div class="note">${escapeHtml(message)}</div>
    </article>
  `;
}

function updateMeta(path) {
  const dataDate = state.raw && state.raw.data_date ? formatDataDate(state.raw.data_date) : "-";
  const history = state.history && state.history.snapshots
    ? ` · ${state.lang === "en" ? "speed history" : "속도 이력"} ${state.history.snapshots.length}${state.lang === "en" ? " days" : "일"}`
    : ` · ${state.lang === "en" ? "no speed history" : "속도 이력 없음"}`;
  els.dataMeta.textContent = state.lang === "en"
    ? `Updated ${dataDate} · source ${path} · OBT rows ${fmt(state.rows.length)}${history}`
    : `업데이트 기준 ${dataDate} · source ${path} · OBT rows ${fmt(state.rows.length)}${history}`;
}

function setOptions(select, options, selected) {
  const values = options.map(([value]) => value);
  const nextSelected = values.includes(selected) ? selected : options[0]?.[0] || "";
  select.innerHTML = options.map(([value, label]) => `<option value="${escapeAttr(value)}">${escapeHtml(label)}</option>`).join("");
  select.value = nextSelected;
}

function uniqueSorted(values) {
  return Array.from(new Set(values.filter(Boolean))).sort((a, b) => a.localeCompare(b));
}

function quantile(values, q) {
  const sorted = values.filter((value) => Number.isFinite(value) && value > 0).sort((a, b) => a - b);
  if (!sorted.length) return 0;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * q) - 1));
  return sorted[index];
}

function quantileAll(values, q) {
  const sorted = values.filter((value) => Number.isFinite(value) && value >= 0).sort((a, b) => a - b);
  if (!sorted.length) return 0;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * q) - 1));
  return sorted[index];
}

function defaultTargetMonth(dataDate) {
  const currentStart = getCurrentWeekStartDate(dataDate);
  return weekToMonth(formatKoreanDate(addDays(currentStart, 7))) || String(dataDate || "").slice(0, 6);
}

function getCurrentWeekStartDate(dataDate = state.raw && state.raw.data_date) {
  const date = parseDataDate(dataDate) || new Date();
  return addDays(date, -date.getDay());
}

function parseDataDate(value) {
  const text = String(value || "");
  if (text.length !== 8) return null;
  return new Date(Number(text.slice(0, 4)), Number(text.slice(4, 6)) - 1, Number(text.slice(6, 8)));
}

function addDays(date, days) {
  const next = new Date(date.getFullYear(), date.getMonth(), date.getDate());
  next.setDate(next.getDate() + days);
  return next;
}

function formatKoreanDate(date) {
  return `${date.getFullYear()}년 ${String(date.getMonth() + 1).padStart(2, "0")}월 ${String(date.getDate()).padStart(2, "0")}일`;
}

function koreanWeekToDate(week) {
  const match = String(week || "").match(/(\d{4})\D+(\d{1,2})\D+(\d{1,2})/);
  if (!match) return new Date();
  return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
}

function weekToMonth(week) {
  const found = state.rows.find((row) => row.week === week && row.month);
  return found ? found.month : "";
}

function weekToWW(week) {
  const month = weekToMonth(week);
  if (month) {
    const weeks = weeksForMonth(month);
    const idx = weeks.indexOf(week);
    if (idx >= 0) {
      const yearMonths = (state.months || []).filter((m) => m.slice(0, 4) === month.slice(0, 4)).sort();
      let ww = idx + 1;
      for (const m of yearMonths) {
        if (m === month) break;
        ww += (state.raw && state.raw.wpm && state.raw.wpm[m]) || weeksForMonth(m).length;
      }
      return String(ww);
    }
  }
  const date = koreanWeekToDate(week);
  const start = new Date(date.getFullYear(), 0, 4);
  const firstSunday = addDays(start, -start.getDay());
  const diff = Math.round((date - firstSunday) / (7 * 86400000));
  return diff >= 0 ? String(diff + 1) : "";
}

function shiftMonth(month, delta) {
  if (!month || month.length !== 6) return "";
  const date = new Date(Number(month.slice(0, 4)), Number(month.slice(4, 6)) - 1 + delta, 1);
  return `${date.getFullYear()}${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function horizonLabel(mode, weeks) {
  const names = state.lang === "en"
    ? { next3: "W+1 to W+3", w1: "W+1", w2: "W+2", w3: "W+3" }
    : { next3: "차주~3주뒤", w1: "차주", w2: "차차주", w3: "3주뒤" };
  return `${names[mode] || (state.lang === "en" ? "Horizon" : "대응주차")} ${weeks.map(weekLabelWithWW).join(", ")}`;
}

function weekLabelWithWW(week) {
  const ww = weekToWW(week);
  return `${shortWeek(week)}${ww ? ` (WW${ww})` : ""}`;
}

function scaleRow(row, divisor) {
  const scaled = { ...row };
  ["teu", "w3Teu", "w3NormTeu", "w3CancelTeu", "w3HiTeu", "w3HiNormTeu", "w3RouteHiTeu", "w3Cm1"].forEach((field) => {
    scaled[field] = (scaled[field] || 0) / divisor;
  });
  return scaled;
}

function sumMap(map, field) {
  let total = 0;
  map.forEach((row) => {
    total += row[field] || 0;
  });
  return total;
}

function distinctCount(rows, field) {
  return new Set(rows.map((row) => row[field]).filter(Boolean)).size;
}

function distinctCountWhen(rows, getter, predicate) {
  const set = new Set();
  rows.forEach((row) => {
    if (!predicate(row)) return;
    const value = getter(row);
    if (value) set.add(value);
  });
  return set.size;
}

function shipperId(row) {
  return row.shipperCode || row.shipperName;
}

function hasSignal(row, signal) {
  return row.issue === signal || (row.signals || []).includes(signal);
}

function getTopShare(route) {
  if (!route || !route.teu) return 0;
  let top = 0;
  route.shipperTeu.forEach((value) => {
    top = Math.max(top, value);
  });
  return top / route.teu;
}

function topIssue(issueMap) {
  let best = "";
  let bestCount = -1;
  issueMap.forEach((count, issue) => {
    if (count > bestCount) {
      best = issue;
      bestCount = count;
    }
  });
  return best || "-";
}

function signalChips(row) {
  return (row.signals || [row.issue])
    .map((issue) => `<span class="issue-chip">${issue}</span>`)
    .join("");
}

function routeIssueChips(issues) {
  const visible = issues.slice(0, 2);
  const rest = Math.max(0, issues.length - visible.length);
  return [
    ...visible.map((issue) => `<span class="issue-chip">${issue}</span>`),
    rest ? `<span class="issue-chip">+${rest}</span>` : ""
  ].join("");
}

function groupIssueSummary(rows) {
  const map = new Map();
  rows.forEach((row) => {
    const group = riskGroup(row.issue);
    const def = RISK_GROUP_DEFS[group] || RISK_GROUP_DEFS["화주 기반 감소"];
    const enGroup = RISK_GROUP_EN[group] || group;
    const found = map.get(group) || {
      group,
      count: 0,
      impactTeu: 0,
      high: 0,
      meaning: state.lang === "en" ? guideRiskGroupMeaning(enGroup) : def.meaning,
      approach: state.lang === "en" ? guideRiskGroupApproach(enGroup) : def.approach,
      children: []
    };
    found.count += row.count;
    found.impactTeu += row.impactTeu;
    found.high += row.high;
    found.children.push(row);
    map.set(group, found);
  });
  return Array.from(map.values())
    .map((row) => ({
      ...row,
      children: row.children.sort((a, b) => b.impactTeu - a.impactTeu || b.count - a.count).slice(0, 4)
    }))
    .sort((a, b) => b.impactTeu - a.impactTeu || b.count - a.count);
}

function riskGroup(issue) {
  if (/리드타임|속도|예상미달/.test(issue)) return "리드타임/속도";
  if (/BSA|선복|목표 Gap|확보 부족|3W\/BSA/.test(issue)) return "BSA/선복 Gap";
  if (/3W|선행/.test(issue)) return "3W 선행 부킹";
  if (/고수익|Late|취소|품질/.test(issue)) return "수익/품질";
  if (/쏠림|신규|급증|변동/.test(issue)) return "집중/변동";
  if (/화주|이탈|감소|급감|TEU 감소|핵심/.test(issue)) return "화주 기반 감소";
  return "화주 기반 감소";
}

function riskLabel(issue) {
  if (state.lang === "en" && RISK_EN[issue]) return RISK_EN[issue].label;
  return issue;
}

function riskGroupLabel(group) {
  if (state.lang === "en") return RISK_GROUP_EN[group] || group;
  return group;
}

function riskMeaning(issue) {
  if (state.lang === "en" && RISK_EN[issue]) return RISK_EN[issue].meaning;
  return (RISK_DEFS[issue] && RISK_DEFS[issue].meaning) || (state.lang === "en" ? "A change requiring action was detected in the selected scope." : "선택 조건에서 조치가 필요한 변동이 감지되었습니다.");
}

function riskApproach(issue) {
  if (state.lang === "en" && RISK_EN[issue]) return RISK_EN[issue].approach;
  return (RISK_DEFS[issue] && RISK_DEFS[issue].approach) || (state.lang === "en" ? "Review the cause by route and customer owner." : "담당 구간과 화주 단위로 변동 사유를 확인합니다.");
}

function issueChip(issue, suffix = "") {
  const text = `${riskLabel(issue)}${suffix}`;
  const title = `${riskLabel(issue)}: ${riskMeaning(issue)} ${riskApproach(issue)}`;
  return `<span class="issue-chip" title="${escapeAttr(title)}">${escapeHtml(text)}</span>`;
}

function priorityTitle(priority) {
  return (PRIORITY_HELP[state.lang] && PRIORITY_HELP[state.lang][priority]) || priority;
}

function salesStatusTitle(row) {
  if (state.lang === "en") {
    if (salesStatusCode(row) === "즉시 대응") return "Immediate: many P1 items, large gap, or multiple trend-risk routes.";
    if (salesStatusCode(row) === "우선 확인") return "Priority Check: action is needed but there is still time to recover.";
    if (salesStatusCode(row) === "화주 회복") return "Customer Recovery: mainly customer decline or win-back candidates.";
    return "Watch: monitor lower-impact signals.";
  }
  if (salesStatusCode(row) === "즉시 대응") return "즉시 대응: P1 항목이 많거나 Gap/트렌드 리스크가 커 당일 확인이 필요합니다.";
  if (salesStatusCode(row) === "우선 확인") return "우선 확인: 회복 시간은 있으나 조치 우선순위에 포함됩니다.";
  if (salesStatusCode(row) === "화주 회복") return "화주 회복: 감소/이탈 화주 중심으로 회복 또는 재확보가 필요합니다.";
  return "관찰: 영향이 낮거나 추세 확인 대상입니다.";
}

function riskMixText(text, fallbackIssue) {
  if (text) return text;
  return riskLabel(fallbackIssue);
}

function actionText(text) {
  if (state.lang !== "en") return text || "";
  const map = {
    "트렌드 지연 구간의 회복/대체 후보를 먼저 확인": "Start with delayed-trend routes and recovery/substitute candidates.",
    "관련 BSA Gap을 회복 후보와 대체 물량으로 분리": "Split the BSA gap into recoverable and substitute volume.",
    "이탈 화주 재확보 가능성과 경쟁사 전환 사유를 우선 확인": "Check win-back feasibility and competitor-switch reasons first.",
    "감소 화주의 회복 가능 잔여 물량을 먼저 확인": "Check recoverable residual volume for declining customers first.",
    "감소 화주의 선적 계획 변경과 회복 TEU 확인": "Confirm plan changes and recoverable TEU with declining customers.",
    "반복 이탈 화주의 운임/선복/스케줄 이슈 확인 후 재확보": "Win back repeat lost customers after checking rate, space, and schedule issues.",
    "3W 선행부킹 미유입 화주를 조기 전환": "Convert customers missing 3W advance bookings earlier.",
    "주요 변동 구간 상태 확인": "Review the top changed route and customer state.",
    "회복/재확보 후보 우선 접촉": "Contact recovery and win-back candidates first.",
    "회복 가능 물량 우선 협의": "Discuss recoverable volume first.",
    "이탈 화주 재확보 가능성 확인": "Check win-back feasibility for lost customers.",
    "선행부킹 전환 후보 확정": "Confirm candidates for advance-booking conversion.",
    "현재 물량 방어 조건 확인": "Check conditions to protect current volume.",
    "대체 물량 또는 BSA 조정 판단": "Decide substitute volume or BSA adjustment.",
    "BSA 적정성/대체 물량 판단": "Review BSA appropriateness and substitute volume.",
    "BSA Gap 기여 후보 선별": "Select customers contributing to the BSA gap.",
    "Gap 회복/대체 물량 병행": "Run gap recovery and substitute volume in parallel.",
    "핵심 화주 회복 가능성 확인": "Check recovery feasibility for key customers.",
    "선행부킹 미유입 화주 확인": "Check customers missing advance bookings.",
    "변동 사유와 회복 가능성 확인": "Check change reasons and recovery feasibility.",
    "리드타임별 회복 후보 확인": "Check recovery candidates by lead time.",
    "회복/선행부킹 가능성 확인": "Check recovery and advance-booking feasibility.",
    "Gap 기여 회복 가능 TEU 확인": "Check recoverable TEU that contributes to the gap.",
    "회복 가능 TEU 확인": "Check recoverable TEU.",
    "추세 관찰": "Monitor trend."
  };
  return map[text] || text || "";
}

function actionDetailText(text) {
  if (state.lang !== "en") return text || "";
  return String(text || "")
    .replace(/회복/g, "recovery ")
    .replace(/재확보/g, "win-back ")
    .replace(/선행/g, "advance ")
    .replace(/방어/g, "protect ")
    .replace(/대체 필요/g, "substitute needed")
    .replace(/곳/g, "")
    .replace(/·/g, "·");
}

function kpiHelp(key) {
  const ko = {
    totalTeu: "현재 선택 기간의 norm_lst 합계입니다. norm_lst가 없으면 fst를 사용합니다.",
    bsaUtil: "현재 TEU를 BSA TEU로 나눈 소석률입니다.",
    paceRisk: "리드타임 트렌드 부족 또는 최근 일별 부킹속도 부족 구간 수입니다.",
    topAction: "현재 필터에서 상위 조치 후보로 노출되는 건수입니다.",
    w3Teu: "shipper.w3_fst 기반 3주전 선행 부킹 TEU 합계입니다.",
    w3Bsa: "3W Booking TEU를 BSA로 나눈 비율입니다.",
    issueCustomers: "감소, 급감, 이탈 등 화주 단위 조치 대상 수입니다.",
    impactTeu: "기준 대비 회복이 필요한 영향 TEU 합계입니다.",
    salesOwners: "조치 대상 화주/구간을 보유한 영업사원 수입니다."
  };
  const en = {
    totalTeu: "Sum of norm_lst for the selected current period. fst is used when norm_lst is missing.",
    bsaUtil: "Current TEU divided by BSA TEU.",
    paceRisk: "Number of routes with lead-time trend shortfall or daily pickup shortfall.",
    topAction: "Number of top action candidates shown under the current filters.",
    w3Teu: "Sum of 3W advance-booked TEU from shipper.w3_fst.",
    w3Bsa: "3W booking TEU divided by BSA.",
    issueCustomers: "Customer-level action targets such as decline, sharp decline, or churn.",
    impactTeu: "Recoverable impact TEU compared with the baseline.",
    salesOwners: "Sales owners with actionable customer or route issues."
  };
  return (state.lang === "en" ? en : ko)[key] || "";
}

function leadTrendLabelText(row) {
  if (row.leadTrendLabel) return row.leadTrendLabel;
  return state.lang === "en" ? `expected ${rpct(row.leadTrendExpectedRate || 0)}` : `기대 ${rpct(row.leadTrendExpectedRate || 0)}`;
}

function guideRiskGroupMeaning(group) {
  const map = {
    "Lead Time / Pace": "Booking maturity or daily pickup is behind the expected timing.",
    "BSA / Space Gap": "Current or projected TEU is below BSA or the target space.",
    "Customer Base Loss": "Repeat customers are lost, sharply declining, or less active.",
    "3W Advance Booking": "Three-week advance booking volume or customer participation is weak.",
    "Profit / Quality": "High-profit volume, late-booking dependency, or cancellation quality needs attention.",
    "Concentration / Volatility": "Volume is concentrated or unusually volatile."
  };
  return map[group] || "A grouped operational risk needs review.";
}

function guideRiskGroupApproach(group) {
  const map = {
    "Lead Time / Pace": "First decide whether this is normal immaturity or a real delay.",
    "BSA / Space Gap": "Separate recoverable volume from substitute volume or BSA adjustment.",
    "Customer Base Loss": "Split customers into recovery candidates and win-back targets.",
    "3W Advance Booking": "Push repeat customers toward earlier booking confirmation.",
    "Profit / Quality": "Prioritize high-profit customers and bookings with cancellation risk.",
    "Concentration / Volatility": "Check recurrence, dependency, and space/equipment coverage."
  };
  return map[group] || "Review routes and customers with the owner.";
}

function paceCell(row) {
  const trendLabels = state.lang === "en"
    ? { "trend-short": "Trend short", "trend-slow": "Trend slow", "trend-ok": "Trend normal" }
    : { "trend-short": "트렌드 부족", "trend-slow": "트렌드 느림", "trend-ok": "트렌드 정상" };
  const hasTrend = row.leadTrendStatus && row.leadTrendStatus !== "no-trend";
  if (!hasTrend && (!state.history || row.pace3 == null)) {
    return `<span class="issue-chip" title="${escapeAttr(state.lang === "en" ? "No history data is available." : "속도 이력이 없어 계산할 수 없습니다.")}">${state.lang === "en" ? "No history" : "이력 없음"}</span><div class="subline">history.json ${state.lang === "en" ? "needed" : "필요"}</div>`;
  }
  const labels = state.lang === "en"
    ? { filled: "Filled", stalled: "Stalled", slow: "Slow", short: "Short", ok: "Normal", watch: "Watch", "no-bsa": "No BSA" }
    : { filled: "충족", stalled: "정체", slow: "느림", short: "부족", ok: "정상", watch: "주의", "no-bsa": "BSA 없음" };
  const tone = ["stalled", "slow", "short"].includes(row.paceStatus)
    ? "neg"
    : row.paceStatus === "ok" || row.paceStatus === "filled"
      ? "pos"
      : "warn";
  const trendTone = row.leadTrendStatus === "trend-short"
    ? "neg"
    : row.leadTrendStatus === "trend-slow"
      ? "warn"
      : "pos";
  const trendBlock = hasTrend ? `
      <span class="${trendTone}" title="${escapeAttr(state.lang === "en" ? "Lead-time maturity compared with destination-port benchmark." : "도착포트별 같은 리드타임 부킹 성숙도 대비 상태입니다.")}">${trendLabels[row.leadTrendStatus] || (state.lang === "en" ? "Trend check" : "트렌드 확인")}</span>
      <span class="subline">${leadTrendLabelText(row)}</span>
    ` : "";
  const paceBlock = state.history && row.pace3 != null ? `
      <span class="subline">${state.lang === "en" ? "Recent" : "최근"} ${fmt(row.pace3 || 0)}/${state.lang === "en" ? "day" : "일"} · ${state.lang === "en" ? "required" : "필요"} ${fmt(row.requiredDaily || 0)}/${state.lang === "en" ? "day" : "일"}</span>
      <span class="subline">${fmt(row.daysRemaining || 0)}${state.lang === "en" ? " days left" : "일 남음"}</span>
    ` : "";
  return `
    <div class="metric-pair">
      ${trendBlock || `<span class="${tone}">${labels[row.paceStatus] || "확인"}</span>`}
      ${paceBlock}
    </div>
  `;
}

function historyLabel() {
  const snapshots = state.history && state.history.snapshots || [];
  if (!snapshots.length) return state.lang === "en" ? "no pace history" : "속도이력 없음";
  return state.lang === "en" ? `${snapshots.length}-day pace` : `최근 ${snapshots.length}일 속도`;
}

function showLoading(show) {
  els.loading.classList.toggle("hidden", !show);
}

function clean(value) {
  return String(value ?? "").trim();
}

function toNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : 0;
}

function parseKoreanDate(value) {
  const match = String(value || "").match(/(\d{4})\D+(\d{1,2})\D+(\d{1,2})/);
  if (!match) return 0;
  return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3])).getTime();
}

function formatMonth(value) {
  if (!value || value.length !== 6) return value || "-";
  return `${value.slice(0, 4)}-${value.slice(4)}`;
}

function shortWeek(value) {
  const match = String(value || "").match(/(\d{4})\D+(\d{1,2})\D+(\d{1,2})/);
  if (!match) return value || "-";
  return `${match[2].padStart(2, "0")}/${match[3].padStart(2, "0")}`;
}

function formatDataDate(value) {
  const text = String(value || "");
  if (text.length !== 8) return text || "-";
  return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
}

function fmt(value) {
  const num = Math.round((Number(value) || 0) * 10) / 10;
  return num.toLocaleString("en-US", { maximumFractionDigits: num % 1 ? 1 : 0 });
}

function signed(value) {
  const num = Math.round((Number(value) || 0) * 10) / 10;
  return `${num > 0 ? "+" : ""}${fmt(num)}`;
}

function pct(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "0%";
  return `${num > 0 ? "+" : ""}${(num * 100).toFixed(Math.abs(num) < .1 ? 1 : 0)}%`;
}

function rpct(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "0%";
  return `${(num * 100).toFixed(Math.abs(num) < .1 ? 1 : 0)}%`;
}

function badgeClass(level) {
  if (level === "high") return "high";
  if (level === "mid") return "mid";
  return "watch";
}

function priorityClass(priority) {
  if (priority === "P1") return "p1";
  if (priority === "P2") return "p2";
  return "p3";
}

function severityLabel(level) {
  if (level === "high") return "High";
  if (level === "mid") return "Mid";
  return "Watch";
}

function emptyRow(colspan, message) {
  return `<tr><td colspan="${colspan}" class="empty-row">${message}</td></tr>`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function escapeAttr(value) {
  return escapeHtml(value).replaceAll("`", "&#096;");
}

function debounce(fn, wait) {
  let timer = null;
  return (...args) => {
    window.clearTimeout(timer);
    timer = window.setTimeout(() => fn(...args), wait);
  };
}
