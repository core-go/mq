package validator

import (
	"context"
	"fmt"
	"reflect"
	"unicode"

	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"

	s "github.com/core-go/mq"
)

type Validator[T any] struct {
	validate           *validator.Validate
	Trans              *ut.Translator
	CustomValidateList []CustomValidate
	IgnoreField        bool
	Map                map[string]string
}

func NewValidator[T any](opts ...bool) (*Validator[T], error) {
	return NewValidatorWithMap[T](nil, opts...)
}
func NewValidatorWithMap[T any](mp map[string]string, opts ...bool) (*Validator[T], error) {
	register := true
	if len(opts) > 0 {
		register = opts[0]
	}
	ignoreField := false
	if len(opts) > 1 {
		ignoreField = opts[1]
	}
	uValidate, uTranslator, err := NewDefaultValidator()
	if err != nil {
		return nil, err
	}
	list := GetCustomValidateList()
	validator := &Validator[T]{Map: mp, validate: uValidate, Trans: &uTranslator, CustomValidateList: list, IgnoreField: ignoreField}
	if register {
		err2 := validator.RegisterCustomValidate()
		if err2 != nil {
			return validator, err2
		}
	}
	return validator, nil
}
func NewDefaultChecker() (*validator.Validate, ut.Translator, error) {
	return NewDefaultValidator()
}
func NewDefaultValidator() (*validator.Validate, ut.Translator, error) {
	validate := validator.New()
	var transl ut.Translator
	if trans != nil {
		transl = *trans
	} else {
		list := GetCustomValidateList()
		for _, v := range list {
			err := validate.RegisterValidation(v.Tag, v.Fn)
			if err != nil {
				return nil, nil, err
			}
		}
		ptr, err := RegisterTranslatorEn(validate)
		if err != nil {
			return nil, nil, err
		}
		transl = ptr
	}
	return validate, transl, nil
}
func (p *Validator[T]) Validate(ctx context.Context, model T) ([]s.ErrorMessage, error) {
	errors := make([]s.ErrorMessage, 0)
	err := p.validate.Struct(model)

	if err != nil {
		errors, err = p.MapErrors(err)
	}
	if p.Map != nil {
		l := len(errors)
		for i := 0; i < l; i++ {
			nv, ok := p.Map[errors[i].Code]
			if ok {
				errors[i].Code = nv
			}
		}
	}
	return errors, err
}

var alias = map[string]string{
	"max":      "maxlength",
	"min":      "minlength",
	"gtefield": "minfield",
	"ltefield": "maxfield",
}

func getTagName(err validator.FieldError) string {
	var code string
	if aliasTag, ok := alias[err.Tag()]; ok {
		if (err.Tag() == "max" || err.Tag() == "min") && err.Kind() != reflect.String {
			code = err.Tag()
		} else {
			code = aliasTag
		}
	} else {
		code = err.Tag()
	}
	if err.Param() != "" {
		code += ":" + lcFirstChar(err.Param())
	}
	return code
}
func lcFirstChar(s string) string {
	if len(s) > 0 {
		runes := []rune(s)
		runes[0] = unicode.ToLower(runes[0])
		return string(runes)
	}
	return s
}
func (p *Validator[T]) RegisterCustomValidate() error {
	for _, v := range p.CustomValidateList {
		err := p.validate.RegisterValidation(v.Tag, v.Fn)
		if err != nil {
			return err
		}
	}
	if p.Trans != nil && p.validate != nil {
		// register default translate
		for _, validate := range p.CustomValidateList {
			if text, ok := translations[validate.Tag]; ok {
				err := AddMessage(p.validate, *p.Trans, validate.Tag, text, true)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (p *Validator[T]) MapErrors(err error) (list []s.ErrorMessage, err1 error) {
	if _, ok := err.(*validator.InvalidValidationError); ok {
		err1 = fmt.Errorf("InvalidValidationError")
		return
	}
	tr := *p.Trans
	for _, err := range err.(validator.ValidationErrors) {
		code := getTagName(err)
		list = append(list, s.ErrorMessage{Field: FormatErrorField(err.Namespace()), Code: code, Message: err.Translate(tr), Param: err.Param()})
	}
	return
}
