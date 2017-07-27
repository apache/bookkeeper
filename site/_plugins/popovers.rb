require 'yaml'

TERMS = YAML.load(File.open("_data/popovers.yaml"))

module Jekyll
  class RenderPopover < Liquid::Tag
    def initialize(tag_name, text, tokens)
      @original_term = text.strip.split(' ').join(' ')
      @term = @original_term.gsub(' ', '-').downcase
      @term = 'entry' if @term == 'entries'
      @term = 'fencing' if @term == 'fence'
      @term = 'striping' if @term == 'stripe' or @term == 'stripedma'
      @term = @term[0...-1] if @term.end_with? 's'
    end

    def render(ctx)
      return "<span class=\"pop\" id=\"#{@term}-popover\">#{@original_term}</span>"
    end
  end
end

Liquid::Template.register_tag('pop', Jekyll::RenderPopover)
