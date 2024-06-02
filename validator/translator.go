package validator

import (
	"github.com/go-playground/locales/en_US"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	tr "github.com/go-playground/validator/v10/translations/en"
	"log"
)

var trans *ut.Translator

func RegisterGlobalTranslator(validate *validator.Validate) {
	t, err := RegisterTranslatorEn(validate)
	if err != nil {
		panic(err)
	}
	trans = &t
}

func RegisterTranslatorEn(validate *validator.Validate) (ut.Translator, error) {
	en := en_US.New()
	uni := ut.New(en, en)
	transEn, _ := uni.GetTranslator("en_US")
	err := tr.RegisterDefaultTranslations(validate, transEn)
	if err != nil {
		return nil, err
	}
	return transEn, nil
}

func AddMessage(v *validator.Validate, trans ut.Translator, tag string, translation string, override bool) error {
	return v.RegisterTranslation(tag, trans, RegistrationFunc(tag, translation, override), TranslateFunc)
}
func RegistrationFunc(tag string, translation string, override bool) validator.RegisterTranslationsFunc {
	return func(ut ut.Translator) (err error) {
		if err = ut.Add(tag, translation, override); err != nil {
			return
		}
		return
	}
}

func TranslateFunc(ut ut.Translator, fe validator.FieldError) string {
	t, err := ut.T(fe.Tag(), fe.Field())
	if err != nil {
		log.Printf("warning: error translating FieldError: %#v", fe)
		return fe.(error).Error()
	}

	return t
}
