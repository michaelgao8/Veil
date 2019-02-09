package Veil

import "testing"

func TestVeilAdd(t *testing.T) {
    cases := []struct{
        Name string
        A, B, Expected float64
        }{
            {"foo", 1.0, 1.0, 2.0 },
            {"bar", 1.0, -1.0, 0.0 },
         }
        for _, tc := range cases {
            t.Run(tc.Name, func(t *testing.T) {
                actual := VeilAdd(tc.A ,tc.B)
                if actual != tc.Expected { t.Fatal("failure") }

            })
            }
}
