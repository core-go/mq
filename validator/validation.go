package validator

import (
	"net"
	"net/url"
	"regexp"
	"strings"
)

var (
	emailPattern = "^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
	phonePattern = `^(?:(?:\(?(?:00|\+)([1-4]\d\d|[1-9]\d?)\)?)?[\-\.\ \\\/]?)?((?:\(?\d{1,}\)?[\-\.\ \\\/]?){0,})(?:[\-\.\ \\\/]?(?:#|ext\.?|extension|x)[\-\.\ \\\/]?(\d+))?$`
	faxPattern   = `^(\+?\d{1,}(\s?|\-?)\d*(\s?|\-?)\(?\d{2,}\)?(\s?|\-?)\d{3,}\s?\d{3,})$`

	regEmail = regexp.MustCompile(emailPattern)
	regPhone = regexp.MustCompile(phonePattern)
	regFax   = regexp.MustCompile(faxPattern)
)

func IsEmpty(v string) bool {
	return len(strings.TrimSpace(v)) == 0
}

func IsDigit(v string) bool {
	// result, _ := regexp.MatchString(`\D+`, v)
	// return !result
	var len = len(v) - 1
	for i := 0; i <= len; i++ {
		var chr = string(v[i])
		if !(chr >= "0" && chr <= "9") {
			return false
		}
	}
	return true
}

func IsDashDigit(v string) bool {
	if IsEmpty(v) == true {
		return false
	}
	var len = len(v) - 1
	for i := 0; i <= len; i++ {
		var chr = string(v[i])
		if !((chr >= "0" && chr <= "9") || chr == "-") {
			return false
		}
	}
	return true
}

func IsAbc(v string) bool {
	if IsEmpty(v) == true {
		return false
	}
	var len = len(v) - 1
	for i := 0; i <= len; i++ {
		var chr = string(v[i])
		if !((chr >= "A" && chr <= "Z") || (chr >= "a" && chr <= "z")) {
			return false
		}
	}
	return true
}

func IsCode(v string) bool {
	if IsEmpty(v) == true {
		return false
	}
	var len = len(v) - 1
	for i := 0; i <= len; i++ {
		var chr = string(v[i])
		if !((chr >= "0" && chr <= "9") || (chr >= "A" && chr <= "Z") || (chr >= "a" && chr <= "z")) {
			return false
		}
	}
	return true
}
func IsUserName(v string) bool {
	if len(v) < 6 {
		return false
	}
	if IsEmail(v) {
		return true
	}
	if IsPhone(v) {
		return true
	}
	if len(v) > 30 {
		return false
	}

	v2 := v + "@gmail.com"
	return IsEmail(v2)
}
func IsDashCode(v string) bool {
	if IsEmpty(v) == true {
		return false
	}
	var len = len(v) - 1
	for i := 0; i <= len; i++ {
		var chr = string(v[i])
		if !((chr >= "0" && chr <= "9") || (chr >= "A" && chr <= "Z") || (chr >= "a" && chr <= "z") || chr == "-" || chr == "_") {
			return false
		}
	}
	return true
}

func IsEmail(v string) bool {
	return regEmail.MatchString(v)
}

func IsUrl(v string) bool {
	u, err := url.Parse(v)
	return err == nil && u.Scheme != "" && u.Host != ""
}
func IsUri(v string) bool {
	_, err := url.ParseRequestURI(v)
	return err == nil
}
func IsIpAddress(s string) bool {
	ip := net.ParseIP(s)
	return ip != nil
}
func IsIpAddressV4(s string) bool {
	ip := net.ParseIP(s)
	return ip != nil && strings.Contains(s, ".")
}
func IsIpAddressV6(s string) bool {
	ip := net.ParseIP(s)
	return ip != nil && strings.Contains(s, ":")
}

func IsValidPattern(pattern string, v string) bool {
	reg := regexp.MustCompile(pattern)
	return reg.MatchString(v)
}

func IsPhone(v string) bool {
	if strings.HasPrefix(v, "+") {
		chars := []rune(v)
		if IsDigit(string(chars[1:])) {
			for i := 2; i <= 4; i++ {
				countryCode := string(chars[1:i])
				if _, ok := PhoneDic[countryCode]; ok {
					return true
				}
			}
			return false
		} else {
			return false
		}
	} else {
		return regPhone.MatchString(v)
	}
}

func IsFax(v string) bool {
	if strings.HasPrefix(v, "+") {
		chars := []rune(v)
		for i := 2; i <= 4; i++ {
			countryCode := string(chars[1:i])
			if _, ok := PhoneDic[countryCode]; ok {
				return regFax.MatchString(v)
			}
		}
		return false
	} else {
		return regFax.MatchString(v)
	}
}

var PhoneDic = map[string]string{
	"1":   "CA:CAN,US:USA",
	"20":  "EG:EGY",
	"211": "SS:SSD",
	"212": "MA:MAR,EH:ESH",
	"213": "DZ:DZA",
	"216": "TN:TUN",
	"218": "LY:LBY",
	"220": "GM:GMB",
	"221": "SN:SEN",
	"222": "MR:MRT",
	"223": "ML:MLI",
	"224": "GN:GIN",
	"225": "CI:CIV",
	"226": "BF:BFA",
	"227": "NE:NER",
	"228": "TG:TGO",
	"229": "BJ:BEN",
	"230": "MU:MUS",
	"231": "LR:LBR",
	"232": "SL:SLE",
	"233": "GH:GHA",
	"234": "NG:NGA",
	"235": "TD:TCD",
	"236": "CF:CAF",
	"237": "CM:CMR",
	"238": "CV:CPV",
	"239": "ST:STP",
	"240": "GQ:GNQ",
	"241": "GA:GAB",
	"242": "CG:COG",
	"243": "CD:COD",
	"244": "AO:AGO",
	"245": "GW:GNB",
	"246": "IO:IOT",
	"248": "SC:SYC",
	"249": "SD:SDN",
	"250": "RW:RWA",
	"251": "ET:ETH",
	"252": "SO:SOM",
	"253": "DJ:DJI",
	"254": "KE:KEN",
	"255": "TZ:TZA",
	"256": "UG:UGA",
	"257": "BI:BDI",
	"258": "MZ:MOZ",
	"260": "ZM:ZMB",
	"261": "MG:MDG",
	"262": "RE:REU,YT:MYT",
	"263": "ZW:ZWE",
	"264": "NA:NAM",
	"265": "MW:MWI",
	"266": "LS:LSO",
	"267": "BW:BWA",
	"268": "SZ:SWZ",
	"269": "KM:COM",
	"27":  "ZA:ZAF",
	"290": "SH:SHN",
	"291": "ER:ERI",
	"297": "AW:ABW",
	"298": "FO:FRO",
	"299": "GL:GRL",
	"30":  "GR:GRC",
	"31":  "NL:NLD",
	"32":  "BE:BEL",
	"33":  "FR:FRA",
	"34":  "ES:ESP",
	"350": "GI:GIB",
	"351": "PT:PRT",
	"352": "LU:LUX",
	"353": "IE:IRL",
	"354": "IS:ISL",
	"355": "AL:ALB",
	"356": "MT:MLT",
	"357": "CY:CYP",
	"358": "FI:FIN",
	"359": "BG:BGR",
	"36":  "HU:HUN",
	"370": "LT:LTU",
	"371": "LV:LVA",
	"372": "EE:EST",
	"373": "MD:MDA",
	"374": "AM:ARM",
	"375": "BY:BLR",
	"376": "AD:AND",
	"377": "MC:MCO",
	"378": "SM:SMR",
	"379": "VA:VAT",
	"380": "UA:UKR",
	"381": "RS:SRB",
	"382": "ME:MNE",
	"383": "XK:XKX",
	"385": "HR:HRV",
	"386": "SI:SVN",
	"387": "BA:BIH",
	"389": "MK:MKD",
	"39":  "IT:ITA",
	"40":  "RO:ROU",
	"41":  "CH:CHE",
	"420": "CZ:CZE",
	"421": "SK:SVK",
	"423": "LI:LIE",
	"43":  "AT:AUT",
	"44":  "GB:GBR",
	"45":  "DK:DNK",
	"46":  "SE:SWE",
	"47":  "NO:NOR,SJ:SJM",
	"48":  "PL:POL",
	"49":  "DE:DEU",
	"500": "FK:FLK",
	"501": "BZ:BLZ",
	"502": "GT:GTM",
	"503": "SV:SLV",
	"504": "HN:HND",
	"505": "NI:NIC",
	"506": "CR:CRI",
	"507": "PA:PAN",
	"508": "PM:SPM",
	"509": "HT:HTI",
	"51":  "PE:PER",
	"52":  "MX:MEX",
	"53":  "CU:CUB",
	"54":  "AR:ARG",
	"55":  "BR:BRA",
	"56":  "CL:CHL",
	"57":  "CO:COL",
	"58":  "VE:VEN",
	"590": "BL:BLM,MF:MAF",
	"591": "BO:BOL",
	"592": "GY:GUY",
	"593": "EC:ECU",
	"595": "PY:PRY",
	"597": "SR:SUR",
	"598": "UY:URY",
	"599": "AN:ANT,CW:CUW",
	"60":  "MY:MYS",
	"61":  "AU:AUS,CC:CCK,CX:CXR",
	"62":  "ID:IDN",
	"63":  "PH:PHL",
	"64":  "NZ:NZL,PN:PCN",
	"65":  "SG:SGP",
	"66":  "TH:THA",
	"670": "TL:TLS",
	"672": "AQ:ATA",
	"673": "BN:BRN",
	"674": "NR:NRU",
	"675": "PG:PNG",
	"676": "TO:TON",
	"677": "SB:SLB",
	"678": "VU:VUT",
	"679": "FJ:FJI",
	"680": "PW:PLW",
	"681": "WF:WLF",
	"682": "CK:COK",
	"683": "NU:NIU",
	"685": "WS:WSM",
	"686": "KI:KIR",
	"687": "NC:NCL",
	"688": "TV:TUV",
	"689": "PF:PYF",
	"690": "TK:TKL",
	"691": "FM:FSM",
	"692": "MH:MHL",
	"7":   "KZ:KAZ,RU:RUS",
	"81":  "JP:JPN",
	"82":  "KR:KOR",
	"84":  "VN:VNM",
	"86":  "CN:CHN",
	"850": "KP:PRK",
	"852": "HK:HKG",
	"853": "MO:MAC",
	"855": "KH:KHM",
	"856": "LA:LAO",
	"880": "BD:BGD",
	"886": "TW:TWN",
	"90":  "TR:TUR",
	"91":  "IN:IND",
	"92":  "PK:PAK",
	"93":  "AF:AFG",
	"94":  "LK:LKA",
	"95":  "MM:MMR",
	"98":  "IR:IRN",
	"960": "MV:MDV",
	"961": "LB:LBN",
	"962": "JO:JOR",
	"963": "SY:SYR",
	"964": "IQ:IRQ",
	"965": "KW:KWT",
	"966": "SA:SAU",
	"967": "YE:YEM",
	"968": "OM:OMN",
	"970": "PS:PSE",
	"971": "AE:ARE",
	"972": "IL:ISR",
	"973": "BH:BHR",
	"974": "QA:QAT",
	"975": "BT:BTN",
	"976": "MN:MNG",
	"977": "NP:NPL",
	"992": "TJ:TJK",
	"993": "TM:TKM",
	"994": "AZ:AZE",
	"995": "GE:GEO",
	"996": "KG:KGZ",
	"998": "UZ:UZB",
}
