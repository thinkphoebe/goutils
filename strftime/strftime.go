package strftime

import (
	"time"
)

var lDay = [...][]byte{[]byte("Sunday"), []byte("Monday"), []byte("Tuesday"), []byte("Wednesday"),
	[]byte("Thursday"), []byte("Friday"), []byte("Saturday")}
var sDay = [...][]byte{[]byte("Sun"), []byte("Mon"), []byte("Tue"), []byte("Wed"), []byte("Thu"), []byte("Fri"), []byte("Sat")}
var sMon = [...][]byte{[]byte("Jan"), []byte("Feb"), []byte("Mar"), []byte("Apr"), []byte("May"), []byte("Jun"),
	[]byte("Jul"), []byte("Aug"), []byte("Sep"), []byte("Oct"), []byte("Nov"), []byte("Dec")}
var lMon = [12][]byte{[]byte("January"), []byte("February"), []byte("March"), []byte("April"), []byte("May"), []byte("June"),
	[]byte("July"), []byte("August"), []byte("September"), []byte("October"), []byte("November"), []byte("December")}
var intToStr = [...][]byte{[]byte("00"), []byte("01"), []byte("02"), []byte("03"), []byte("04"), []byte("05"),
	[]byte("06"), []byte("07"), []byte("08"), []byte("09"), []byte("10"), []byte("11"), []byte("12"), []byte("13"),
	[]byte("14"), []byte("15"), []byte("16"), []byte("17"), []byte("18"), []byte("19"), []byte("20"), []byte("21"),
	[]byte("22"), []byte("23"), []byte("24"), []byte("25"), []byte("26"), []byte("27"), []byte("28"), []byte("29"),
	[]byte("30"), []byte("31"),
}

// Cheap integer to fixed-width decimal ASCII. Give a negative width to avoid zero-padding.
func itoa(buf *[]byte, i int, wid int) {
	// Assemble decimal in reverse order.
	var b [10]byte
	bp := len(b) - 1
	for i >= 10 || wid > 1 {
		wid--
		q := i / 10
		b[bp] = byte('0' + i - q*10)
		bp--
		i = q
	}
	// i < 10
	b[bp] = byte('0' + i)
	*buf = append(*buf, b[bp:]...)
}

func Strftime(f string, t *time.Time) string {
	var buf [64]byte
	var result = buf[:0]
	var year, day = -1, -1
	var mon time.Month
	var hour, min, sec = -1, -1, -1
	var zoneOffset = -1
	var zoneName string

	for i := 0; i < len(f); i++ {
		if f[i] == '%' {
			if i < len(f)-1 {
				switch f[i+1] {
				case 'a':
					result = append(result, sDay[t.Weekday()]...)
				case 'A':
					result = append(result, lDay[t.Weekday()]...)
				case 'w':
					result = append(result, byte('0'+t.Weekday()))
				case 'd':
					if year == -1 {
						year, mon, day = t.Date()
					}
					result = append(result, intToStr[day]...)
				case 'b':
					if year == -1 {
						year, mon, day = t.Date()
					}
					result = append(result, sMon[mon-1]...)
				case 'B':
					if year == -1 {
						year, mon, day = t.Date()
					}
					result = append(result, lMon[mon-1]...)
				case 'm':
					if year == -1 {
						year, mon, day = t.Date()
					}
					result = append(result, intToStr[mon]...)
				case 'y':
					if year == -1 {
						year, mon, day = t.Date()
					}
					itoa(&result, year%100, 2)
				case 'Y':
					if year == -1 {
						year, mon, day = t.Date()
					}
					itoa(&result, year, 4)
				case 'H':
					if hour < 0 {
						hour, min, sec = t.Clock()
					}
					result = append(result, intToStr[hour]...)
				case 'I':
					if hour < 0 {
						hour, min, sec = t.Clock()
					}
					if hour == 0 {
						result = append(result, intToStr[12]...)
					} else if hour > 12 {
						result = append(result, intToStr[hour-12]...)
					} else {
						result = append(result, intToStr[hour]...)
					}
				case 'p':
					if hour < 0 {
						hour, min, sec = t.Clock()
					}
					if hour < 12 {
						result = append(result, "AM"...)
					} else {
						result = append(result, "PM"...)
					}
				case 'M':
					if hour < 0 {
						hour, min, sec = t.Clock()
					}
					itoa(&result, min, 2)
				case 'S':
					if hour < 0 {
						hour, min, sec = t.Clock()
					}
					itoa(&result, sec, 2)
				case 'f':
					itoa(&result, t.Nanosecond()/1000, 6)
				case 'z':
					if zoneName == "" {
						zoneName, zoneOffset = t.Zone()
					}
					zone := zoneOffset / 60 // convert to minutes
					if zone < 0 {
						result = append(result, '-')
						zone = -zone
					} else {
						result = append(result, '+')
					}
					itoa(&result, zone/60, 2)
					itoa(&result, zone%60, 2)
				case 'Z':
					if zoneName == "" {
						zoneName, zoneOffset = t.Zone()
					}
					result = append(result, zoneName...)
				case 'j':
					itoa(&result, t.YearDay(), 3)
				case 'U':
					result = append(result, intToStr[(t.YearDay()+6-int(t.Weekday()))/7]...)
				case 'W':
					weekday := int(t.Weekday())
					if weekday == 0 {
						weekday = 6
					} else {
						weekday -= 1
					}
					result = append(result, intToStr[(t.YearDay()+6-weekday)/7]...)
				case 'c':
					result = append(result, []byte(Strftime("%a %b %e %H:%M:%S %Y", t))...)
				case 'x':
					result = append(result, []byte(Strftime("%m/%d/%Y", t))...)
				case 'X':
					result = append(result, []byte(Strftime("%H:%M:%S", t))...)

				case '%':
					result = append(result, "%"...)
				case 'e':
					var buf []byte
					if year == -1 {
						year, mon, day = t.Date()
					}
					itoa(&buf, day, -1)
					if len(buf) == 1 {
						result = append(result, " "...)
					}
					result = append(result, buf...)
				default:
					result = append(result, f[i:i+2]...)
				}
				i += 1
			} else {
				result = append(result, "%"...)
			}
		} else {
			result = append(result, f[i])
		}
	}

	return string(result)
}
