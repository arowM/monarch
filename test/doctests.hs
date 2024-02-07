import Test.DocTest

main :: IO ()
main =
  doctest
    [ "-isrc",
      "-ignore-package",
      "monads-tf"
      -- , "Database.Monarch.Utils"
    ]
