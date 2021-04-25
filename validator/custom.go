package validator

import "github.com/go-playground/validator/v10"

type CustomValidate struct {
	Fn  validator.Func
	Tag string
}

var PatternMap = map[string]string{
	"digit":      "^\\d+$",
	"dash_digit": "^[0-9-]*$",
	"code":       "^\\w*\\d*$",
}

func GetCustomValidateList() (list []CustomValidate) {
	list = append(list, CustomValidate{Fn: CheckEmail, Tag: "email"})
	list = append(list, CustomValidate{Fn: CheckUrl, Tag: "url"})
	list = append(list, CustomValidate{Fn: CheckUri, Tag: "uri"})
	list = append(list, CustomValidate{Fn: CheckFax, Tag: "fax"})
	list = append(list, CustomValidate{Fn: CheckPhone, Tag: "phone"})
	list = append(list, CustomValidate{Fn: CheckIp, Tag: "ip"})
	list = append(list, CustomValidate{Fn: CheckIpV4, Tag: "ipv4"})
	list = append(list, CustomValidate{Fn: CheckIpV6, Tag: "ipv6"})
	list = append(list, CustomValidate{Fn: CheckDigit, Tag: "digit"})
	list = append(list, CustomValidate{Fn: CheckAbc, Tag: "abc"})
	list = append(list, CustomValidate{Fn: CheckId, Tag: "id"})
	list = append(list, CustomValidate{Fn: CheckCode, Tag: "code"})
	list = append(list, CustomValidate{Fn: CheckCountryCode, Tag: "country_code"})
	list = append(list, CustomValidate{Fn: CheckUsername, Tag: "username"})
	list = append(list, CustomValidate{Fn: CheckPattern, Tag: "regex"})
	return
}
func CheckString(fl validator.FieldLevel, fn func(string) bool) bool {
	s := fl.Field().String()
	if len(s) == 0 {
		return true
	}
	return fn(s)
}
func CheckEmail(fl validator.FieldLevel) bool {
	return CheckString(fl, IsEmail)
}
func CheckUrl(fl validator.FieldLevel) bool {
	return CheckString(fl, IsUrl)
}
func CheckUri(fl validator.FieldLevel) bool {
	return CheckString(fl, IsUri)
}
func CheckFax(fl validator.FieldLevel) bool {
	return CheckString(fl, IsFax)
}
func CheckPhone(fl validator.FieldLevel) bool {
	return CheckString(fl, IsPhone)
}
func CheckIp(fl validator.FieldLevel) bool {
	return CheckString(fl, IsIpAddress)
}
func CheckIpV4(fl validator.FieldLevel) bool {
	return CheckString(fl, IsIpAddressV4)
}
func CheckIpV6(fl validator.FieldLevel) bool {
	return CheckString(fl, IsIpAddressV6)
}
func CheckDigit(fl validator.FieldLevel) bool {
	return CheckString(fl, IsDigit)
}
func CheckAbc(fl validator.FieldLevel) bool {
	return CheckString(fl, IsAbc)
}
func CheckId(fl validator.FieldLevel) bool {
	return CheckString(fl, IsCode)
}
func CheckCode(fl validator.FieldLevel) bool {
	return CheckString(fl, IsDashCode)
}
func CheckCountryCode(fl validator.FieldLevel) bool {
	return CheckString(fl, IsCountryCode)
}
func CheckUsername(fl validator.FieldLevel) bool {
	return CheckString(fl, IsUserName)
}
func CheckPattern(fl validator.FieldLevel) bool {
	param := fl.Param()
	if pattern, ok := PatternMap[param]; ok {
		return IsValidPattern(pattern, fl.Field().String())
	} else {
		panic("invalid pattern")
	}
}
